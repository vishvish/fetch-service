use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter};
use std::io::prelude::*;

use chrono::{DateTime, Utc};
use chrono::prelude::*;
use futures::StreamExt;
use log::{error, info};

fn read_lines(path: &str) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    Ok(
        reader.lines().filter_map(Result::ok).collect()
    )
}

async fn write_file(filename: &str, data: String) -> Result<(), Box<dyn Error + Send + Sync>> {
    let utc: DateTime<Utc> = Utc::now();
    let folder: String = format!("data/{year}/{month}/{day}/", year = utc.year(), month = utc.month(), day = utc.day());
    let _ = fs::create_dir_all(&folder)?;
    let mut buffer = BufWriter::new(File::create(folder + filename)?);
    buffer.write_all(data.as_bytes())?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // the urls.txt contains a link to the latest version of every file
    let urls = read_lines("urls.txt")?;
    let fetches = futures::stream::iter(
        urls.into_iter().map(|url| {
            async move {
                match reqwest::get(&url).await {
                    Ok(resp) => {
                        match resp.text().await {
                            Ok(text) => {
                                let mut base = url.split('/');
                                let filename = base.next_back();
                                println!("RESPONSE: {} bytes from {}", text.len(), url);
                                let _res = write_file(filename.unwrap(), text).await;
                            }
                            Err(_) => error!("ERROR reading {}", url),
                        }
                    }
                    Err(_) => error!("ERROR downloading {}", url),
                }
            }
        })
    ).buffer_unordered(50).collect::<Vec<()>>();
    info!("Waiting...");
    fetches.await;
    Ok(())
}