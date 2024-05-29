use bitcoin::Transaction;
use index_btc::model;
use tokio::{sync::Semaphore, task};

use std::sync::Arc;

pub async fn process_txs(parallelism: usize, txs: Vec<Transaction>) -> Vec<model::SumTx> {
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
