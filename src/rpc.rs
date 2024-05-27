use crate::log;
use bitcoin::Transaction;
use bitcoincore_rpc::{Auth, Client, RpcApi};
use chrono::DateTime;
use futures::stream::StreamExt;
use index_btc::model;
use tokio::{sync::Semaphore, task};
use tokio_stream::Stream; // Add this line to import the `model` module

use std::sync::Arc;

type Height = u64;

pub struct RpcClient {
    rpc_client: Arc<Client>,
}
impl RpcClient {
    pub fn new(rpc_url: String, username: String, password: String) -> Self {
        let user_pass = Auth::UserPass(username, password);
        let rpc = Arc::new(Client::new(&rpc_url, user_pass).unwrap());
        RpcClient { rpc_client: rpc }
    }

    pub fn fetch_blocks(
        &self,
        num_cores: usize,
        start_height: Height,
        end_height: Height,
    ) -> impl Stream<Item = Result<(Height, Vec<model::SumTx>), String>> + '_ {
        let heights = start_height..=end_height;
        let parallelism = num_cores / 2;
        tokio_stream::iter(heights)
            .map(move |height| {
                let rpc_client = self.rpc_client.clone();
                task::spawn_blocking(move || {
                    // Get the block hash at the specified height
                    let block_hash = rpc_client
                        .get_block_hash(height)
                        .map_err(|e| e.to_string())?;

                    // Get the block by its hash
                    let block = rpc_client
                        .get_block(&block_hash)
                        .map_err(|e| e.to_string())?;

                    // print the block hash if height is divisible by 1000
                    if height % 1000 == 0 {
                        let datetime =
                            DateTime::from_timestamp(block.header.time as i64, 0).unwrap();
                        let readable_date = datetime.format("%Y-%m-%d %H:%M:%S").to_string();
                        log!("Block @ {} : {} : {}", readable_date, height, block_hash);
                    }
                    Ok::<(u64, bitcoin::Block), String>((height, block))
                })
            })
            .buffered(128) // Process up to 16 blocks in parallel
            // process_txs
            .map(move |result| async move {
                match result {
                    Ok(Ok((height, block))) => {
                        let sum_txs = process_txs(parallelism, block.txdata).await;
                        Ok((height, sum_txs))
                    }
                    Ok(Err(e)) => Err(e),
                    Err(e) => Err(e.to_string()),
                }
            })
            .buffered(64)
    }
}

async fn process_txs(parallelism: usize, txs: Vec<Transaction>) -> Vec<model::SumTx> {
    let batch_size = 100;
    let sem = Arc::new(Semaphore::new(parallelism)); // Limit to 4 concurrent batches

    let tasks: Vec<_> = txs
        .chunks(batch_size)
        .map(|chunk| {
            let sem = Arc::clone(&sem);
            let chunk = chunk.to_vec();
            task::spawn(async move {
                let _permit = sem.acquire().await.unwrap(); // Acquire a permit asynchronously
                task::spawn_blocking(move || {
                    chunk
                        .into_iter()
                        .map(|tx| model::SumTx::from(tx))
                        .collect::<Vec<_>>()
                })
                .await
                .unwrap()
            })
        })
        .collect();

    let results = futures::future::join_all(tasks).await;

    results
        .into_iter()
        .flat_map(|result| result.expect("Batch processing task panicked"))
        .collect()
}
