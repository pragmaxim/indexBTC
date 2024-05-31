use index_btc::indexer::{Indexer, IndexerError};
use index_btc::model::{SumTx, Utxo, ADDRESS_CF, CACHE_CF, LAST_HEIGHT_KEY};
use rocksdb::{MultiThreaded, Options, TransactionDB, TransactionDBOptions};
use std::str;
use std::sync::{Arc, RwLock};

pub struct RocksDbIndexer {
    db: Arc<RwLock<TransactionDB<MultiThreaded>>>,
}

// Derive Clone for AddressIndexer
impl Clone for RocksDbIndexer {
    fn clone(&self) -> RocksDbIndexer {
        RocksDbIndexer {
            db: Arc::clone(&self.db),
        }
    }
}

impl RocksDbIndexer {
    // Method to process the outputs of a transaction
    fn process_outputs(
        &self,
        sum_tx: &SumTx,
        db_tx: &rocksdb::Transaction<TransactionDB<MultiThreaded>>,
        batch: &mut rocksdb::WriteBatchWithTransaction<true>,
        address_cf: &Arc<rocksdb::BoundColumnFamily>,
        cache_cf: &Arc<rocksdb::BoundColumnFamily>,
    ) -> Result<(), rocksdb::Error> {
        for utxo in sum_tx.outs.iter() {
            let tx_id_with_index = format!("{}|{}", &sum_tx.txid, utxo.index);
            let utxo_bytes = utxo.to_string().into_bytes();
            db_tx.put_cf(cache_cf, tx_id_with_index, utxo_bytes)?;
            let address_key = format!("{}|{}|{}|{}", utxo.address, "O", &sum_tx.txid, utxo.index);
            batch.put_cf(address_cf, address_key, utxo.value.to_ne_bytes());
        }
        Ok(())
    }

    // Method to process the inputs of a transaction
    fn process_inputs(
        &self,
        sum_tx: &SumTx,
        db_tx: &rocksdb::Transaction<TransactionDB<MultiThreaded>>,
        batch: &mut rocksdb::WriteBatchWithTransaction<true>,
        address_cf: &Arc<rocksdb::BoundColumnFamily>,
        cache_cf: &Arc<rocksdb::BoundColumnFamily>,
    ) -> Result<(), rocksdb::Error> {
        for indexed_txid in &sum_tx.ins {
            let tx_cache_key = indexed_txid.to_string();
            let utxo_str = db_tx.get_cf(cache_cf, tx_cache_key)?.unwrap();
            let utxo: Utxo = Utxo::try_from(utxo_str).unwrap();
            let address_key = format!(
                "{}|{}|{}|{}",
                utxo.address, "I", indexed_txid.tx_id, indexed_txid.index
            );
            batch.put_cf(address_cf, address_key, utxo.value.to_ne_bytes());
        }
        Ok(())
    }
}

impl Indexer for RocksDbIndexer {
    fn get_last_height(&self) -> u64 {
        return self
            .db
            .clone()
            .read()
            .unwrap()
            .get(LAST_HEIGHT_KEY)
            .unwrap()
            .map_or(0, |height| {
                String::from_utf8(height).unwrap().parse::<u64>().unwrap()
            });
    }

    fn update_balance(&mut self, height: u64, sum_txs: &Vec<SumTx>) -> Result<(), IndexerError> {
        let db_arc = self.db.clone();
        let db = db_arc.write().unwrap();
        let db_tx = db.transaction();
        let address_cf = db.cf_handle(ADDRESS_CF).unwrap();
        let cache_cf = db.cf_handle(CACHE_CF).unwrap();
        let mut batch = db_tx.get_writebatch();
        for sum_tx in sum_txs {
            self.process_outputs(&sum_tx, &db_tx, &mut batch, &address_cf, &cache_cf)?;
            if !sum_tx.is_coinbase {
                self.process_inputs(sum_tx, &db_tx, &mut batch, &address_cf, &cache_cf)?;
            }
        }
        db_tx.put(LAST_HEIGHT_KEY, height.to_string().as_bytes())?;
        db_tx.commit()?;
        Ok(())
    }

    fn new(num_cores: i32, db_path: &str) -> Result<Self, IndexerError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        // Increase parallelism: setting the number of background threads
        opts.increase_parallelism(num_cores / 2); // Set this based on your CPU cores
        opts.set_max_background_jobs(std::cmp::max(num_cores / 2, 6));
        // Set other options for performance
        opts.set_max_file_opening_threads(std::cmp::max(num_cores / 2, 6));
        opts.set_write_buffer_size(128 * 1024 * 1024); // 64 MB
        opts.set_max_write_buffer_number(8);
        opts.set_target_file_size_base(128 * 1024 * 1024); // 64 MB
        opts.set_max_bytes_for_level_base(512 * 1024 * 1024);
        opts.set_use_direct_io_for_flush_and_compaction(true);

        let cfs =
            rocksdb::TransactionDB::<MultiThreaded>::list_cf(&opts, db_path).unwrap_or(vec![]);

        let txn_db_opts = TransactionDBOptions::default();
        let instance =
            TransactionDB::open_cf(&opts, &txn_db_opts, db_path.to_string(), &cfs).unwrap();
        if cfs.iter().find(|cf| cf == &CACHE_CF).is_none() {
            let options = rocksdb::Options::default();
            instance.create_cf(CACHE_CF, &options).unwrap();
            instance.create_cf(ADDRESS_CF, &options).unwrap();
        }
        Ok(RocksDbIndexer {
            db: Arc::new(RwLock::new(instance)),
        })
    }
}
