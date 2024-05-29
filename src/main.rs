use core::panic;
use futures::stream::StreamExt;
use std::env;

mod logger;
mod merkle;
mod process;
mod rpc;

use clap::{Arg, ArgAction, Command};

fn cli() -> Command {
    Command::new("indexBTC")
        .about("Bitcoin transactions indexer")
        .version("1.0")
        .author("Pragmaxim <pragmaxim@gmail.com>")
        .args([
            Arg::new("db-path")
                .long("db-path")
                .allow_hyphen_values(true)
                .require_equals(true)
                .action(ArgAction::Set)
                .num_args(1)
                .default_value("/tmp/index_btc.db")
                .help("Absolute path to db directory"),
            Arg::new("btc-url")
                .long("btc-url")
                .action(ArgAction::Set)
                .require_equals(true)
                .allow_hyphen_values(true)
                .num_args(1)
                .default_value("http://127.0.0.1:8332")
                .help("Url of local bitcoin-core"),
        ])
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let matches = cli().get_matches();

    let bitcoin_url = matches.get_one::<String>("btc-url").unwrap();
    log!("Connecting to bitcoin-core at : {}", bitcoin_url);

    let db_path = matches.get_one::<String>("db-path").unwrap();
    log!("Using db path : {}", db_path);

    let (username, password) = match (
        env::var("BITCOIN_RPC_USERNAME"),
        env::var("BITCOIN_RPC_PASSWORD"),
    ) {
        (Ok(user), Ok(pass)) => (user, pass),
        _ => {
            panic!("Error: Bitcoin RPC BITCOIN_RPC_PASSWORD or BITCOIN_RPC_USERNAME environment variable not set");
        }
    };

    let num_cores = num_cpus::get();
    log!("Number of CPU cores: {}", num_cores);

    let mut merkle_sum_tree = merkle::AddressIndexer::new(num_cores as i32, db_path).unwrap();
    let rpc_client = rpc::RpcClient::new(bitcoin_url.clone(), username, password);

    let from_height: u64 = merkle_sum_tree.get_last_height() + 1;
    let end_height: u64 = 844566;

    let parallelism = num_cores / 2;
    log!(
        "Initiating syncing from {} to {} with parallelism {}",
        from_height,
        end_height,
        parallelism
    );
    let blocks_count = rpc_client
        .fetch_blocks(from_height, end_height)
        .map(|result| async move {
            match result {
                Ok((height, block)) => {
                    let sum_txs = process::process_txs(parallelism, block.txdata).await;
                    Ok((height, sum_txs))
                }
                Err(e) => Err(e.to_string()),
            }
        })
        .buffered(128)
        .map(|result| match result {
            Ok((height, sum_txs)) => {
                merkle_sum_tree
                    .update_balance(height, &sum_txs)
                    .map_err(|e| e.to_string())
                    .unwrap(); // Handle the Err variant by unwrapping the Result
                (height, sum_txs.len())
            }
            Err(e) => {
                panic!("Error: {}", e);
            }
        })
        .fold(0 as u64, |acc, (height, tx_count)| async move {
            if height % 1000 == 0 {
                log!("Block @ {} with {} txs, {} total", height, tx_count, acc);
            }
            acc + tx_count as u64
        })
        .await;

    log!("Processed {} blocks", blocks_count);
    return Ok(());
}
