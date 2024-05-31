use crate::model::SumTx;

// define new module indexer

#[derive(Debug)]
pub enum IndexerError {
    RocksDbError(String),
    SledError(String),
}

impl From<rocksdb::Error> for IndexerError {
    fn from(error: rocksdb::Error) -> Self {
        IndexerError::RocksDbError(error.to_string())
    }
}

impl From<std::io::Error> for IndexerError {
    fn from(error: std::io::Error) -> Self {
        IndexerError::RocksDbError(error.to_string())
    }
}

pub trait Indexer {
    fn update_balance(&mut self, height: u64, sum_txs: &Vec<SumTx>) -> Result<(), IndexerError>;
    fn get_last_height(&self) -> u64;
    fn new(num_cores: i32, db_path: &str) -> Result<Self, IndexerError>
    where
        Self: Sized;
}
