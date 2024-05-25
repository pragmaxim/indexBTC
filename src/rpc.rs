use bitcoin::Transaction;
use bitcoincore_rpc::{Auth, Client, RpcApi};
use chrono::DateTime;
use futures::stream::StreamExt;
use index_btc::model;
use tokio::task;
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
        start_height: Height,
        end_height: Height,
    ) -> impl Stream<Item = Result<(Height, Vec<model::SumTx>), String>> + '_ {
        let heights = start_height..=end_height;
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
                        println!("Block @ {} : {} : {}", readable_date, height, block_hash);
                    }
                    Ok::<(u64, bitcoin::Block), String>((height, block))
                })
            })
            .buffered(32) // Process up to 16 blocks in parallel
            // process_txs
            .map(|result| async {
                match result {
                    Ok(Ok((height, block))) => {
                        let sum_txs = process_txs(block.txdata).await;
                        Ok((height, sum_txs))
                    }
                    Ok(Err(e)) => Err(e),
                    Err(e) => Err(e.to_string()),
                }
            })
            .buffered(64)
    }
}

async fn process_txs(txs: Vec<Transaction>) -> Vec<model::SumTx> {
    let futures: Vec<tokio::task::JoinHandle<model::SumTx>> = txs
        .into_iter()
        .map(|tx| tokio::task::spawn_blocking(move || model::SumTx::from(tx)))
        .collect();

    let sum_txs: Vec<model::SumTx> = futures::future::join_all(futures)
        .await
        .into_iter()
        .map(|res| res.expect("Task panicked"))
        .collect();

    sum_txs
}
