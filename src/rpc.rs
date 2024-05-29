use crate::log;
use bitcoincore_rpc::{Auth, Client, RpcApi};
use chrono::DateTime;
use futures::stream::StreamExt;
use tokio::task::{self, JoinError};
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
    ) -> impl Stream<Item = Result<(Height, bitcoin::Block), JoinError>> + '_ {
        let heights = start_height..=end_height;
        tokio_stream::iter(heights)
            .map(move |height| {
                let rpc_client = self.rpc_client.clone();
                task::spawn_blocking(move || {
                    // Get the block hash at the specified height
                    let block_hash = rpc_client.get_block_hash(height).unwrap();

                    // Get the block by its hash
                    let block = rpc_client.get_block(&block_hash).unwrap();

                    // print the block hash if height is divisible by 1000
                    if height % 1000 == 0 {
                        let datetime =
                            DateTime::from_timestamp(block.header.time as i64, 0).unwrap();
                        let readable_date = datetime.format("%Y-%m-%d %H:%M:%S").to_string();
                        log!("Block @ {} : {} : {}", height, readable_date, block_hash);
                    }
                    (height, block)
                })
            })
            .buffered(128)
    }
}
