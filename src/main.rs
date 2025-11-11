use std::time::Duration;

use anyhow::Context;
use backoff::ExponentialBackoff;
use indicatif::{ProgressBar, ProgressStyle};
use rusty_s3::{Bucket, S3Action, UrlStyle, actions::ListObjectsV2};

/// Estimated total number of images in the bucket
const TOTAL_NUMBER_OF_IMAGES: u64 = 100_000_000;

/// Duration for which the presigned URL is valid
const PRESIGNED_URL_DURATION: Duration = Duration::from_secs(60 * 60);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // setting up a bucket
    let url = "https://s3.amazonaws.com";
    let endpoint = url.parse().with_context(|| format!("While parsing the url: {url}"))?;
    let path_style = UrlStyle::VirtualHost;
    let name = "multimedia-commons";
    let region = "us-west-2";
    let bucket = Bucket::new(endpoint, path_style, name, region)
        .context("While opening the bucket with the scheme and host")?;

    // Create an HTTP client
    let client = reqwest::ClientBuilder::new().build()?;

    let style = ProgressStyle::with_template("[{elapsed}] {wide_bar} {pos}/{len} ({eta})").unwrap();
    let pb = ProgressBar::new(TOTAL_NUMBER_OF_IMAGES).with_style(style);
    let mut continuation_token = None;
    loop {
        let mut list_action = bucket.list_objects_v2(None);
        list_action.with_prefix("data/images");
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

        pb.inc(list.contents.len() as u64);

        continuation_token = list.next_continuation_token.take();
        if continuation_token.is_none() {
            break;
        }
    }

    pb.finish();

    Ok(())
}
