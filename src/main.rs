use futures::stream::StreamExt;
use index_btc::model::SumTx;
use std::env;
use tokio::task;

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

    let merkle_sum_tree = merkle::AddressIndexer::new("/tmp/index_btc.db").unwrap();
    let rpc_client = rpc::RpcClient::new(bitcoin_url, username, password);

    let last_height = merkle_sum_tree.get_last_height();
    let from_height: u64 = last_height + 1;
    let end_height: u64 = 844566;

    println!("Initiating syncing from {} to {}", from_height, end_height);
    let _ = rpc_client
        .fetch_blocks(from_height, end_height)
        .map({
            let merkle_sum_tree = merkle_sum_tree.clone();
            move |result| {
                let mut tree = merkle_sum_tree.clone();
                task::spawn_blocking(move || match result {
                    Ok((height, transactions)) => {
                        tree.update_outputs(&transactions).unwrap();
                        Ok::<(u64, Vec<SumTx>), String>((height, transactions))
                    }
                    Err(e) => Result::Err(e),
                })
            }
        })
        .buffered(8)
        .map({
            let merkle_sum_tree = merkle_sum_tree.clone();
            move |result| {
                let mut tree = merkle_sum_tree.clone();
                task::spawn_blocking(move || match result {
                    Ok(Ok((height, transactions))) => {
                        tree.update_inputs(height, &transactions).unwrap();
                        Ok::<(u64, Vec<SumTx>), String>((height, transactions))
                    }
                    Ok(Err(e)) => Result::Err(e),
                    Err(e) => Result::Err(e.to_string()),
                })
            }
        })
        .buffered(8)
        .for_each(|result| async {
            match result {
                Ok(Ok((_, _))) => {}
                Ok(Err(e)) => {
                    eprintln!("Error: {}", e);
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                }
            }
        })
        .await;

    return Ok(());
}
