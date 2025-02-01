extern crate spider;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::{fs, path::Path};
use crate::spider::http_cache_reqwest::CacheManager;
use crate::spider::tokio::io::AsyncWriteExt;
use spider::string_concat::{string_concat, string_concat_impl};
use spider::tokio;
use spider::website::Website;
use spider_utils::spider_transformations::transformation::content::{
    transform_content, ReturnFormat, TransformConfig,
};

static GLOBAL_URL_COUNT: AtomicUsize = AtomicUsize::new(0);

#[tokio::main]
async fn main() {
    let raw = false;
     // Create output directory if it doesn't exist
     let output_dir = Path::new("output");
     if !output_dir.exists() {
         fs::create_dir_all(output_dir).expect("Failed to create output directory");
     }
    let mut website: Website = Website::new("https://www.okcupid.com")
        .with_caching(true)
        .build()
        .unwrap();

    let start = std::time::Instant::now();
    website.crawl_sitemap().await;
    website.persist_links();
    let mut conf = TransformConfig::default();
    conf.return_format = ReturnFormat::Markdown;
    let mut rx2 = website.subscribe(500).unwrap();
 
    let subscription = async move {
        while let Ok(res) = rx2.recv().await {
            let mut stdout = tokio::io::stdout();
            let cache_url = string_concat!("GET:", res.get_url());
              // Create a sanitized filename from the URL
              let url = res.get_url();
              let sanitized_filename = url
                  .replace("https://", "")
                  .replace("http://", "")
                  .replace("/", "_")
                  .replace(":", "_")
                  .replace("?", "_")
                  .replace("&", "_")
                  .replace("=", "_") + ".md";
              
              let output_path = output_dir.join(&sanitized_filename);
            tokio::task::spawn(async move {
                let result = tokio::time::timeout(Duration::from_millis(60), async {
                    spider::website::CACACHE_MANAGER.get(&cache_url).await
                })
                .await;
                let markup = transform_content(&res, &conf, &None, &None, &None);
                 // Save markup to file using tokio's async file operations
                
                    match tokio::fs::write(&output_path, markup).await {
                        Ok(_) => {
                            let message = format!("Saved - {:?} to {:?}\n", cache_url, output_path);
                            let _ = stdout.write_all(message.as_bytes()).await;
                        },
                        Err(e) => {
                            let message = format!("Failed to save {:?}: {:?}\n", cache_url, e);
                            let _ = stdout.write_all(message.as_bytes()).await;
                        }
                    }
                
                match result {
                    Ok(Ok(Some(_cache))) => {
                        let message = format!("HIT - {:?}\n", cache_url);
                        let _ = stdout.write_all(message.as_bytes()).await;
                    }
                    Ok(Ok(None)) | Ok(Err(_)) => {
                        let message = format!("MISS - {:?}\n", cache_url);
                        let _ = stdout.write_all(message.as_bytes()).await;
                    }
                    Err(_) => {
                        let message = format!("ERROR - {:?}\n", cache_url);
                        let _ = stdout.write_all(message.as_bytes()).await;
                    }
                };

                GLOBAL_URL_COUNT.fetch_add(1, Ordering::Relaxed);
            });
        }
    };

    let crawl = async move {
        if raw {
            website.crawl_raw().await;
        }else{
            website.crawl_smart().await;
        }
        website.unsubscribe();
    };

    tokio::pin!(subscription);

    tokio::select! {
        _ = crawl => (),
        _ = subscription => (),
    };

    let duration = start.elapsed();

    println!(
        "Time elapsed in website.crawl() is: {:?} for total pages: {:?}",
        duration, GLOBAL_URL_COUNT
    )
}
