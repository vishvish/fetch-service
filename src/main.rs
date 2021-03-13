use std::fs;
use std::path::{Path, PathBuf};
use std::fs::File;
use std::io::{BufRead, BufReader};

use chrono::prelude::*;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use tokio::io::AsyncWriteExt;
use http::StatusCode;
use log::info;

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Download failed, unexpected response code {0} ({1})")]
    DownloadFail(String, String),

    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

type Result<T> = std::result::Result<T, Error>;

fn read_lines<P: AsRef<Path>>(path: P) -> Result<Vec<(String, PathBuf)>> {
    let file = File::open(path.as_ref())?;
    let reader = BufReader::new(file);
    let mut urls = Vec::new();
    for line in reader.lines() {
        let url = line?;
        let url = url.trim().to_string();
        if !url.is_empty() {
            let dest = destination(&url)?;
            urls.push((url, dest));
        }
    }
    Ok(urls)
}

fn destination(url: &str) -> Result<PathBuf> {
    let utc: DateTime<Utc> = Utc::now();
    let mut base = url.split('/');
    let filename = base.next_back().unwrap();
    let folder = PathBuf::from("data")
        .join(utc.year().to_string())
        .join(utc.month().to_string())
        .join(utc.day().to_string());
    let target = folder.join(filename);
    if !folder.exists() {
        fs::create_dir_all(&folder)?;
    }
    Ok(target)
}

async fn download<P: AsRef<Path>>(url: &str, dest: P) -> Result<()> {
    info!("Download {}", url);
    let mut response = reqwest::get(url).await?;
    if response.status() != StatusCode::OK {
        return Err(Error::DownloadFail(
            url.to_string(),
            response.status().to_string(),
        ));
    }
    let file = File::create(dest.as_ref())?;
    let mut content_file = tokio::fs::File::from_std(file);
    while let Some(chunk) = response.chunk().await? {
        content_file.write_all(&chunk).await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").ok().is_none() {
        std::env::set_var("RUST_LOG", "info");
    }
    pretty_env_logger::init_custom_env("RUST_LOG");

    // the urls.txt contains a link to the latest version of every file
    let urls = read_lines("urls.txt")?;
    let fetches = futures::stream::iter(urls.iter().map(|(url, dest)| download(url, dest)))
        .buffer_unordered(50)
        .collect::<Vec<Result<()>>>();
    info!("Waiting...");
    fetches.await;
    Ok(())
}
