use std::fs;
use std::path::{Path, PathBuf};
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufRead, BufReader, BufWriter};

use chrono::prelude::*;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use log::{error, info};

type Error = Box<dyn std::error::Error + Send + Sync>;
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

async fn write_file<P: AsRef<Path>>(dest: P, data: String) -> Result<()> {
    let mut buffer = BufWriter::new(File::create(dest.as_ref())?);
    buffer.write_all(data.as_bytes())?;
    Ok(())
}

async fn download<P: AsRef<Path>>(url: &str, dest: P) -> Result<()> {
    match reqwest::get(url).await {
        Ok(resp) => match resp.text().await {
            Ok(text) => {
                println!("RESPONSE: {} bytes from {}", text.len(), url);
                let _res = write_file(dest.as_ref(), text).await;
            }
            Err(_) => error!("ERROR reading {}", url),
        },
        Err(_) => error!("ERROR downloading {}", url),
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // the urls.txt contains a link to the latest version of every file
    let urls = read_lines("urls.txt")?;
    let fetches = futures::stream::iter(urls.iter().map(|(url, dest)| download(url, dest)))
        .buffer_unordered(50)
        .collect::<Vec<Result<()>>>();
    info!("Waiting...");
    fetches.await;
    Ok(())
}
