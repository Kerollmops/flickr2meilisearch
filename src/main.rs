use std::borrow::Cow;
use std::io;
use std::pin::pin;
use std::sync::mpsc::{Receiver as StdReceiver, SyncSender as StdSyncSender};
use std::{num::NonZeroUsize, time::Duration};

use anyhow::Context;
use backoff::ExponentialBackoff;
use base64::Engine as _;
use base64::prelude::BASE64_STANDARD;
use byte_unit::Byte;
use bytes::Bytes;
use clap::Parser;
use futures::stream::{self, StreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use meilisearch_sdk::client::Client as MeiliClient;
use path_slash::CowExt as _;
use rayon::iter::{ParallelBridge, ParallelIterator};
use reqwest::header::AUTHORIZATION;
use rusty_s3::{
    Bucket, S3Action, UrlStyle,
    actions::{ListObjectsV2, list_objects_v2::ListObjectsContent},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::error;

/// Program to sync Flickr images to Meilisearch
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// URL of the Meilisearch instance
    #[arg(long)]
    meilisearch_url: String,

    /// API key for the Meilisearch instance
    #[arg(long)]
    meilisearch_api_key: Option<String>,

    /// Dry run mode: Do not send any request to Meilisearch.
    #[arg(long)]
    dry_run: bool,

    /// Estimated total number of images in the bucket
    #[arg(long, default_value = "100000000")]
    total_images: u64,

    /// Duration for which the presigned URL is valid (in seconds)
    #[arg(long, default_value = "3600")]
    presigned_url_duration_secs: u64,

    /// Maximum number of downloads in flight at any given time
    #[arg(long, default_value = "2000")]
    max_in_flight_downloads: usize,

    /// Maximum size of a chunk to send to Meilisearch
    #[arg(long, default_value = "10MiB")]
    meili_chunk_size: Byte,

    /// The frequency in terms of number of sent images at which we clear images from Meilisearch
    #[arg(long, default_value = "100000")]
    clear_images_frequency: u64,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> anyhow::Result<()> {
    let Args {
        meilisearch_url,
        meilisearch_api_key,
        dry_run,
        total_images,
        presigned_url_duration_secs,
        max_in_flight_downloads,
        meili_chunk_size,
        clear_images_frequency,
    } = Args::parse();

    // install global subscriber configured based on RUST_LOG envvar.
    tracing_subscriber::fmt::init();

    // setting up a bucket
    let url = "https://s3.amazonaws.com";
    let endpoint = url.parse().with_context(|| format!("While parsing the url: {url}"))?;
    let path_style = UrlStyle::VirtualHost;
    let name = "multimedia-commons";
    let region = "us-west-2";
    let bucket = Bucket::new(endpoint, path_style, name, region)
        .context("While opening the bucket with the scheme and host")?;

    // Create an HTTP client and a Meilisearch client
    let client = reqwest::ClientBuilder::new().build()?;
    let meili_client = MeiliClient::new(meilisearch_url, meilisearch_api_key)
        .context("While creating the Meilisearch client")?;

    let (images_to_download_sender, images_to_download_receiver) = tokio::sync::mpsc::channel(200);
    let (images_to_compute_base64_sender, images_to_compute_base64_receiver) =
        std::sync::mpsc::sync_channel(200);
    let (base64_to_upload_sender, base64_to_upload_receiver) = tokio::sync::mpsc::channel(200);

    let iterator_pb = ProgressBar::new(total_images).with_style(
        ProgressStyle::with_template(
            "[{elapsed}] Iterator {wide_bar} {human_pos}/~{human_len} ({eta})",
        )
        .unwrap(),
    );
    let downloaded_pb = ProgressBar::new_spinner().with_style(
        ProgressStyle::with_template("[{elapsed}] Downloader {binary_bytes_per_sec}").unwrap(),
    );
    let base64_encoder_pb = ProgressBar::new(total_images).with_style(
        ProgressStyle::with_template(
            "[{elapsed}] Base64 Encoder {wide_bar} {human_pos}/~{human_len} ({eta})",
        )
        .unwrap(),
    );
    let uploader_pb = ProgressBar::new_spinner().with_style(
        ProgressStyle::with_template("[{elapsed}] Uploader {binary_bytes_per_sec}").unwrap(),
    );

    let mpb = MultiProgress::new();
    let iterator_pb = mpb.add(iterator_pb);
    let downloaded_pb = mpb.add(downloaded_pb);
    let base64_encoder_pb = mpb.add(base64_encoder_pb);
    let uploader_pb = mpb.add(uploader_pb);

    let iterator_handle = tokio::spawn({
        let client = client.clone();
        let bucket = bucket.clone();
        let presigned_url_duration = Duration::from_secs(presigned_url_duration_secs);
        async move {
            iterate_through_objects(
                client,
                bucket,
                iterator_pb,
                images_to_download_sender,
                presigned_url_duration,
            )
            .await
        }
    });

    let downloaded_handle = tokio::spawn({
        let client = client.clone();
        let presigned_url_duration = Duration::from_secs(presigned_url_duration_secs);
        let max_in_flight = NonZeroUsize::new(max_in_flight_downloads).unwrap();
        async move {
            download_images(
                client,
                bucket,
                downloaded_pb,
                images_to_download_receiver,
                images_to_compute_base64_sender,
                presigned_url_duration,
                max_in_flight,
            )
            .await
        }
    });

    let compute_base64_handle = tokio::task::spawn_blocking(move || {
        let pool = rayon::ThreadPoolBuilder::new().build()?;
        pool.install(|| {
            compute_images_base64(
                base64_encoder_pb,
                // Dry running will make the pipeline go up to this point
                // and don't send to Meilisearch
                dry_run,
                images_to_compute_base64_receiver,
                base64_to_upload_sender,
            )
        })
    });

    let uploader_handle = tokio::spawn({
        let meili_chunk_size = meili_chunk_size.as_u64() as usize;
        let clear_frequency = clear_images_frequency;
        async move {
            images_uploader(
                client,
                meili_client,
                uploader_pb,
                base64_to_upload_receiver,
                meili_chunk_size,
                clear_frequency,
            )
            .await
        }
    });

    // Waits by itself
    let (iterator_result, downloaded_result, compute_base64_result, uploader_result) =
        tokio::join!(iterator_handle, downloaded_handle, compute_base64_handle, uploader_handle);

    // Try to get the results
    iterator_result??;
    downloaded_result??;
    compute_base64_result??;
    uploader_result??;

    Ok(())
}

async fn iterate_through_objects(
    client: reqwest::Client,
    bucket: Bucket,
    pb: ProgressBar,
    images_to_download_sender: Sender<ListObjectsContent>,
    presigned_url_duration: Duration,
) -> anyhow::Result<()> {
    let mut continuation_token = None;
    loop {
        let mut list_action = bucket.list_objects_v2(None);
        list_action.with_prefix("data/images");
        list_action.with_max_keys(1000);
        if let Some(token) = continuation_token.take() {
            list_action.with_continuation_token(token);
        }

        let mut list = backoff::future::retry(ExponentialBackoff::default(), || async {
            // signing a request
            let signed_action = list_action.sign(presigned_url_duration);
            let response = client
                .get(signed_action)
                .send()
                .await
                .context("While sending HTTP list objects v2 request")?;
            let text = response
                .text()
                .await
                .context("While reading the text from an HTTP list objects v2 request")?;
            let list = ListObjectsV2::parse_response(text)
                .context("While parsing a list objects v2 XML response")?;
            Ok(list)
        })
        .await?;

        for content in list.contents {
            images_to_download_sender.send(content).await?;
            pb.inc(1);
        }

        continuation_token = list.next_continuation_token.take();
        if continuation_token.is_none() {
            break;
        }
    }

    pb.finish();

    Ok(())
}

async fn download_images(
    client: reqwest::Client,
    bucket: Bucket,
    pb: ProgressBar,
    images_to_download_receiver: Receiver<ListObjectsContent>,
    images_to_compute_base64_sender: StdSyncSender<(ListObjectsContent, Bytes)>,
    presigned_url_duration: Duration,
    max_in_flight_downloads: NonZeroUsize,
) -> anyhow::Result<()> {
    // Create a stream from the receiver
    let download_stream = stream::unfold(images_to_download_receiver, |mut receiver| async move {
        receiver.recv().await.map(|content| (content, receiver))
    });

    // Use buffer_unordered to manage in-flight downloads
    let download_futures = download_stream
        .map(|content| {
            let bucket = bucket.clone();
            let client = client.clone();
            async move {
                let request_result = backoff::future::retry(ExponentialBackoff::default(), {
                    let bucket = bucket.clone();
                    let client = client.clone();
                    let content = content.clone();
                    move || {
                        let bucket = bucket.clone();
                        let client = client.clone();
                        let content = content.clone();
                        async move {
                            let get_action = bucket.get_object(None, content.key.as_str());
                            // signing a request
                            let signed_action = get_action.sign(presigned_url_duration);
                            let response = client
                                .get(signed_action)
                                .send()
                                .await
                                .context("While sending HTTP get object request")?;
                            let jpeg_image = response.bytes().await.context(
                                "While reading the bytes from an HTTP get object request",
                            )?;
                            Ok((content, jpeg_image))
                        }
                    }
                })
                .await;
                request_result
            }
        })
        .buffer_unordered(max_in_flight_downloads.get());

    // Pin the stream to satisfy Unpin trait requirement
    let mut download_futures = pin!(download_futures);

    // Process completed downloads
    while let Some(result) = download_futures.next().await {
        let (content, jpeg_bytes) = result?;
        let bytes_to_download = jpeg_bytes.len();

        // Send to base64 computation
        tokio::task::spawn_blocking({
            let images_to_compute_base64_sender = images_to_compute_base64_sender.clone();
            move || images_to_compute_base64_sender.send((content, jpeg_bytes))
        })
        .await??;

        pb.inc(bytes_to_download as u64);
    }

    pb.finish();

    Ok(())
}

fn compute_images_base64(
    pb: ProgressBar,
    dry_run: bool,
    images_to_compute_base64_receiver: StdReceiver<(ListObjectsContent, Bytes)>,
    base64_to_upload_sender: Sender<(ListObjectsContent, String)>,
) -> anyhow::Result<()> {
    images_to_compute_base64_receiver.into_iter().par_bridge().try_for_each(|(content, bytes)| {
        let base64 = BASE64_STANDARD.encode(&bytes);
        if !dry_run {
            base64_to_upload_sender
                .blocking_send((content, base64))
                .context("While sending the base64 to the uploader")?;
        }
        pb.inc(1);
        Ok(())
    })
}

#[derive(Debug, Serialize, Deserialize)]
struct Image {
    id: String,
    url: String,
    base64: String,
}

async fn images_uploader(
    client: reqwest::Client,
    meili_client: MeiliClient,
    pb: ProgressBar,
    mut base64_to_upload_receiver: Receiver<(ListObjectsContent, String)>,
    meili_chunk_size: usize,
    clear_images_frequency: u64,
) -> anyhow::Result<()> {
    let images = meili_client.index("images");
    let edit_documents_to_clear_images = json!({
        "filter": null,
        "function": "doc._vectors = #{ bedrock: #{ regenerate: false } }; doc.remove(\"base64\")"
    });

    let mut buffer = Vec::new();
    let mut current_chunk_size = 0;
    while let Some((content, base64)) = base64_to_upload_receiver.recv().await {
        let ListObjectsContent { key, .. } = content;
        let id = match Cow::from_slash(&key).file_stem() {
            Some(stem) => stem.to_string_lossy().to_string(),
            None => {
                error!("Image key `{key}` does not have a valid file stem");
                continue;
            }
        };

        let image = Image {
            id,
            url: format!("https://multimedia-commons.s3-us-west-2.amazonaws.com/data/images/{key}"),
            base64,
        };

        let mut bytes_counter = BytesCounter::default();
        serde_json::to_writer(&mut bytes_counter, &image)?;
        let BytesCounter { count } = bytes_counter;

        if current_chunk_size + count >= meili_chunk_size {
            backoff::future::retry(backoff::ExponentialBackoff::default(), || async {
                images
                    .add_documents(&buffer, None)
                    .await
                    .context("While sending a chunk of images to Meilisearch")?;
                Ok(())
            })
            .await?;
            pb.inc(current_chunk_size as u64);
            buffer.clear();
            current_chunk_size = 0;
        }

        if pb.position() != 0 && pb.position() % clear_images_frequency == 0 {
            send_clear_images_from_documents(
                &client,
                &meili_client,
                &edit_documents_to_clear_images,
            )
            .await?;
        }

        buffer.push(image);
        current_chunk_size += count;
    }

    // Don't forget to send the last chunk
    if !buffer.is_empty() {
        backoff::future::retry(backoff::ExponentialBackoff::default(), || async {
            images
                .add_documents(&buffer, None)
                .await
                .context("While sending a chunk of images to Meilisearch")?;
            Ok(())
        })
        .await?;
    }

    // Just to make sure we don't send a request if we are in dry-run mode
    if pb.position() != 0 {
        send_clear_images_from_documents(&client, &meili_client, &edit_documents_to_clear_images)
            .await?;
    }

    Ok(())
}

/// Sends a edit-documents-by-function request to remove the images from documents.
async fn send_clear_images_from_documents(
    client: &reqwest::Client,
    meili_client: &MeiliClient,
    edit_documents_to_clear_images: &serde_json::Value,
) -> anyhow::Result<()> {
    let host = meili_client.get_host();
    let url = format!("{host}/indexes/images/documents/edit");

    let request_builder = client.post(url);
    let request_builder = match meili_client.get_api_key() {
        Some(api_key) => request_builder.header(AUTHORIZATION, format!("Bearer {api_key}")),
        None => request_builder,
    };

    let response = request_builder
        .json(edit_documents_to_clear_images)
        .send()
        .await
        .context("While sending a request to clear images")?;
    let status = response.status();
    let text = response.text().await.context("While parsing response from Meilisearch")?;
    if !status.is_success() {
        error!("Failed to clear images: {text}");
    }

    Ok(())
}

/// BytesCounter is a struct that counts the number of bytes written to it.
#[derive(Default, Debug)]
struct BytesCounter {
    pub count: usize,
}

impl io::Write for BytesCounter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.count += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
