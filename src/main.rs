use std::borrow::Cow;
use std::io;
use std::sync::mpsc::{Receiver as StdReceiver, SyncSender as StdSyncSender};
use std::{collections::VecDeque, num::NonZeroUsize, time::Duration};

use anyhow::Context;
use backoff::ExponentialBackoff;
use base64::Engine as _;
use base64::prelude::BASE64_STANDARD;
use bytes::Bytes;
use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use meilisearch_sdk::client::Client as MeiliClient;
use path_slash::CowExt as _;
use rayon::iter::{ParallelBridge, ParallelIterator};
use rusty_s3::{
    Bucket, S3Action, UrlStyle,
    actions::{ListObjectsV2, list_objects_v2::ListObjectsContent},
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::error;

/// Estimated total number of images in the bucket
const TOTAL_NUMBER_OF_IMAGES: u64 = 100_000_000;

/// Duration for which the presigned URL is valid
const PRESIGNED_URL_DURATION: Duration = Duration::from_secs(60 * 60);

/// Maximum number of downloads in flight at any given time
const MAX_IN_FLIGHT_DOWNLOADS: NonZeroUsize = NonZeroUsize::new(2000).unwrap();

/// Maximum size of a chunk to send to Meilisearch
const MEILI_CHUNK_SIZE: usize = 1024 * 1024 * 10; // 10 MiB

/// Simple program to greet a person
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
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> anyhow::Result<()> {
    let Args { meilisearch_url, meilisearch_api_key, dry_run } = Args::parse();

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

    let iterator_pb = ProgressBar::new(TOTAL_NUMBER_OF_IMAGES).with_style(
        ProgressStyle::with_template(
            "[{elapsed}] Iterator {wide_bar} {human_pos}/~{human_len} ({eta})",
        )
        .unwrap(),
    );
    let downloaded_pb = ProgressBar::new(TOTAL_NUMBER_OF_IMAGES).with_style(
        ProgressStyle::with_template("[{elapsed}] Downloader {binary_bytes_per_sec}").unwrap(),
    );
    let base64_encoder_pb = ProgressBar::new(TOTAL_NUMBER_OF_IMAGES).with_style(
        ProgressStyle::with_template(
            "[{elapsed}] Base64 Encoder {wide_bar} {human_pos}/~{human_len} ({eta})",
        )
        .unwrap(),
    );
    let uploader_pb = ProgressBar::new(TOTAL_NUMBER_OF_IMAGES).with_style(
        ProgressStyle::with_template(
            "[{elapsed}] Uploader {wide_bar} {human_pos}/~{human_len} ({eta})",
        )
        .unwrap(),
    );

    let mpb = MultiProgress::new();
    let iterator_pb = mpb.add(iterator_pb);
    let downloaded_pb = mpb.add(downloaded_pb);
    let base64_encoder_pb = mpb.add(base64_encoder_pb);
    let uploader_pb = mpb.add(uploader_pb);

    let iterator_handle = tokio::spawn({
        let client = client.clone();
        let bucket = bucket.clone();
        async move {
            iterate_through_objects(client, bucket, iterator_pb, images_to_download_sender).await
        }
    });

    let downloaded_handle = tokio::spawn({
        let client = client.clone();
        async move {
            download_images(
                client,
                bucket,
                downloaded_pb,
                images_to_download_receiver,
                images_to_compute_base64_sender,
            )
            .await
        }
    });

    let compute_base64_handle = tokio::task::spawn_blocking(|| {
        let pool = rayon::ThreadPoolBuilder::new().build()?;
        pool.install(|| {
            compute_images_base64(
                base64_encoder_pb,
                images_to_compute_base64_receiver,
                base64_to_upload_sender,
            )
        })
    });

    let uploader_handle = tokio::spawn({
        // Strange but when the dry_run option is enabled, we simply create
        // an immediately dropped channel receiver in place of original/normal receiver
        let base64_to_upload_receiver =
            if dry_run { tokio::sync::mpsc::channel(1).1 } else { base64_to_upload_receiver };
        async move { images_uploader(meili_client, uploader_pb, base64_to_upload_receiver).await }
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
            let signed_action = list_action.sign(PRESIGNED_URL_DURATION);
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
    mut images_to_download_receiver: Receiver<ListObjectsContent>,
    images_to_compute_base64_sender: StdSyncSender<(ListObjectsContent, Bytes)>,
) -> anyhow::Result<()> {
    let mut in_flight = VecDeque::with_capacity(MAX_IN_FLIGHT_DOWNLOADS.get());
    while let Some(content) = images_to_download_receiver.recv().await {
        if in_flight.len() >= MAX_IN_FLIGHT_DOWNLOADS.get() {
            let request = in_flight.pop_front().unwrap();
            let (content, jpeg_bytes): (_, Bytes) = request.await?;
            let bytes_to_download = jpeg_bytes.len();
            // NOTE: Not sure about this part. It would be preferable to put
            //       it in another in flight to-be-base64-encoded dequeue.
            tokio::task::spawn_blocking({
                let images_to_compute_base64_sender = images_to_compute_base64_sender.clone();
                move || images_to_compute_base64_sender.send((content, jpeg_bytes))
            })
            .await??;
            pb.inc(bytes_to_download as u64);
        }

        let request = backoff::future::retry(ExponentialBackoff::default(), {
            // NOTE: There is a lot too many clones for my taste
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
                    let signed_action = get_action.sign(PRESIGNED_URL_DURATION);
                    let response = client
                        .get(signed_action)
                        .send()
                        .await
                        .context("While sending HTTP list objects v2 request")?;
                    let jpeg_image = response
                        .bytes()
                        .await
                        .context("While reading the bytes from an HTTP list objects v2 request")?;
                    Ok((content, jpeg_image))
                }
            }
        });

        in_flight.push_back(request);
    }

    for request in in_flight {
        let (content, jpeg_bytes) = request.await?;
        let bytes_to_download = jpeg_bytes.len();
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
    images_to_compute_base64_receiver: StdReceiver<(ListObjectsContent, Bytes)>,
    base64_to_upload_sender: Sender<(ListObjectsContent, String)>,
) -> anyhow::Result<()> {
    images_to_compute_base64_receiver.into_iter().par_bridge().try_for_each(|(content, bytes)| {
        let base64 = BASE64_STANDARD.encode(&bytes);
        base64_to_upload_sender
            .blocking_send((content, base64))
            .context("While sending the base64 to the uploader")?;
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
    meili_client: MeiliClient,
    pb: ProgressBar,
    mut base64_to_upload_receiver: Receiver<(ListObjectsContent, String)>,
) -> anyhow::Result<()> {
    let images = meili_client.index("images");

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

        if current_chunk_size + count >= MEILI_CHUNK_SIZE {
            backoff::future::retry(backoff::ExponentialBackoff::default(), || async {
                images
                    .add_documents(&buffer, None)
                    .await
                    .context("While sending a chunk of images to Meilisearch")?;
                pb.inc(1);
                Ok(())
            })
            .await?;
            buffer.clear();
            current_chunk_size = 0;
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
            pb.inc(1);
            Ok(())
        })
        .await?;
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
