use futures::future;
use futures::StreamExt;
use std::env;

mod merkle;
mod rpc;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    // read bitcoin url from arguments
    let bitcoin_url = env::args()
        .nth(1)
        .unwrap_or("http://127.0.0.1:8332".to_string());

    // Create a new MerkleSumTree instance

    let (username, password) = match (
        env::var("BITCOIN_RPC_USERNAME"),
        env::var("BITCOIN_RPC_PASSWORD"),
    ) {
        (Ok(user), Ok(pass)) => (user, pass),
        _ => {
            panic!("Error: Bitcoin RPC BITCOIN_RPC_PASSWORD or BITCOIN_RPC_USERNAME environment variable not set");
        }
    };

    let mut merkle_sum_tree = merkle::AddressIndexer::new("/tmp/grove.db").unwrap();
    let rpc_client = rpc::RpcClient::new(bitcoin_url, username, password);

    let last_height = merkle_sum_tree.get_last_height();
    let from_height: u64 = last_height + 1;
    let end_height: u64 = 844566;

    println!("Initiating syncing from {} to {}", from_height, end_height);
    let _ = rpc_client
        .fetch_blocks(from_height, end_height)
        .for_each(|result| match result {
            Ok((height, transactions)) => {
                merkle_sum_tree
                    .update_balances(height, transactions)
                    .unwrap();
                future::ready(())
            }
            Err(e) => {
                panic!("Error fetching block: {}", e);
            }
        })
        .await;

    return Ok(());
}
