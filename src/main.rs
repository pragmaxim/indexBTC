use core::panic;
use futures::stream::StreamExt;
use index_btc::indexer::Indexer;
use rocksdb::RocksDbIndexer;
use sleddb::SledDbIndexer;
use std::{env, ops::Deref};

mod logger;
mod process;
mod rocksdb;
mod rpc;
mod sleddb;

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
                .default_value("/tmp/index_btc")
                .help("Absolute path to db directory"),
            Arg::new("btc-url")
                .long("btc-url")
                .action(ArgAction::Set)
                .require_equals(true)
                .allow_hyphen_values(true)
                .num_args(1)
                .default_value("http://127.0.0.1:8332")
                .help("Url of local bitcoin-core"),
            Arg::new("db-engine")
                .long("db-engine")
                .action(ArgAction::Set)
                .require_equals(true)
                .allow_hyphen_values(true)
                .num_args(1)
                .default_value("rocks-db")
                .help("rocks-db or sled-db"),
        ])
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let matches = cli().get_matches();

    let bitcoin_url = matches.get_one::<String>("btc-url").unwrap();
    log!("Connecting to bitcoin-core at : {}", bitcoin_url);

    let db_path = matches.get_one::<String>("db-path").unwrap();
    log!("Using db path : {}", db_path);

    let num_cores = num_cpus::get();
    log!("Number of CPU cores: {}", num_cores);

    let db_engine = matches
        .get_one::<String>("db-engine")
        .map(|s| s.deref())
        .unwrap();
    log!("Using db engine : {}", db_engine);
    let full_db_path = format!("{}/{}", db_path, db_engine);
    let mut indexer: Box<dyn Indexer> = match db_engine {
        "rocks-db" => Box::new(RocksDbIndexer::new(num_cores as i32, &full_db_path).unwrap()),
        "sled-db" => Box::new(SledDbIndexer::new(num_cores as i32, &full_db_path).unwrap()),
        x => panic!("Error: db-engine {} not supported", x),
    };

    let (username, password) = match (
        env::var("BITCOIN_RPC_USERNAME"),
        env::var("BITCOIN_RPC_PASSWORD"),
    ) {
        (Ok(user), Ok(pass)) => (user, pass),
        _ => {
            panic!("Error: Bitcoin RPC BITCOIN_RPC_PASSWORD or BITCOIN_RPC_USERNAME environment variable not set");
        }
    };

    let rpc_client = rpc::RpcClient::new(bitcoin_url.clone(), username, password);

    let from_height: u64 = indexer.get_last_height() + 1;
    let end_height: u64 = 844566;
    let start_time = std::time::Instant::now();
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
                indexer.update_balance(height, &sum_txs).unwrap();
                (height, sum_txs.len())
            }
            Err(e) => {
                panic!("Error: {}", e);
            }
        })
        .fold(0 as u64, |total_tx_count, (height, tx_count)| async move {
            if height % 1000 == 0 {
                let total_time = start_time.elapsed().as_secs();
                let txs_per_sec = format!("{:.1}", total_tx_count as f64 / total_time as f64);
                log!("Indexing Speed: {} txs/sec", txs_per_sec);
            }
            total_tx_count + tx_count as u64
        })
        .await;

    log!("Processed {} blocks", blocks_count);
    return Ok(());
}
