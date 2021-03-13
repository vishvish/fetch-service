use std::fs;
use std::path::PathBuf;
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufRead, BufReader, BufWriter};

use chrono::prelude::*;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use log::{error, info};

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

fn read_lines(path: &str) -> Result<Vec<String>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    Ok(reader.lines().filter_map(std::result::Result::ok).collect())
}

async fn write_file(filename: &str, data: String) -> Result<()> {
    let utc: DateTime<Utc> = Utc::now();
    let folder = PathBuf::from("data")
        .join(utc.year().to_string())
        .join(utc.month().to_string())
        .join(utc.day().to_string());
    let target = folder.join(filename);
    let _ = fs::create_dir_all(&folder)?;
    let mut buffer = BufWriter::new(File::create(&target)?);
    buffer.write_all(data.as_bytes())?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // the urls.txt contains a link to the latest version of every file
    let urls = read_lines("urls.txt")?;
    let fetches = futures::stream::iter(urls.into_iter().map(|url| async move {
        match reqwest::get(&url).await {
            Ok(resp) => match resp.text().await {
                Ok(text) => {
                    let mut base = url.split('/');
                    let filename = base.next_back();
                    println!("RESPONSE: {} bytes from {}", text.len(), url);
                    let _res = write_file(filename.unwrap(), text).await;
                }
                Err(_) => error!("ERROR reading {}", url),
            },
            Err(_) => error!("ERROR downloading {}", url),
        }
    }))
    .buffer_unordered(50)
    .collect::<Vec<()>>();
    info!("Waiting...");
    fetches.await;
    Ok(())
}
