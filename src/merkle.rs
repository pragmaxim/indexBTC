use index_btc::model::{SumTx, Utxo, LAST_HEIGHT_KEY};
use rocksdb::{Options, TransactionDB, TransactionDBOptions};
use std::str;
use std::sync::{Arc, RwLock};

pub struct AddressIndexer {
    db: Arc<RwLock<TransactionDB>>,
}

// Derive Clone for AddressIndexer
impl Clone for AddressIndexer {
    fn clone(&self) -> AddressIndexer {
        AddressIndexer {
            db: Arc::clone(&self.db),
        }
    }
}

impl AddressIndexer {
    // Constructor function to create a new MerkleSumTree instance
    pub fn new(db_path: &str) -> Result<Self, rocksdb::Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let txn_db_opts = TransactionDBOptions::default();
        let db = TransactionDB::open(&opts, &txn_db_opts, db_path.to_string()).unwrap();
        Ok(AddressIndexer {
            db: Arc::new(RwLock::new(db)),
        })
    }

    // Method to insert or update a UTXO for an address
    fn insert_utxo(
        &self,
        tx_id: &str,
        utxo: &Utxo,
        db_tx: &rocksdb::Transaction<TransactionDB>,
    ) -> Result<(), rocksdb::Error> {
        let tx_id_with_index = format!("{}|{}", tx_id, utxo.index);
        db_tx.put(tx_id_with_index, utxo.to_string().into_bytes())?;
        let address_key = format!("{}|{}|{}|{}", utxo.address, "O", tx_id, utxo.index);
        db_tx.put(address_key, utxo.value.to_ne_bytes())?;
        Ok(())
    }

    // Method to process the outputs of a transaction
    fn process_outputs(
        &self,
        sum_tx: &SumTx,
        db_tx: &rocksdb::Transaction<TransactionDB>,
    ) -> Result<(), rocksdb::Error> {
        for utxo in sum_tx.outs.iter() {
            self.insert_utxo(&sum_tx.txid, utxo, db_tx)?;
        }
        Ok(())
    }

    // Method to process the inputs of a transaction
    fn process_inputs(
        &self,
        sum_tx: &SumTx,
        db_tx: &rocksdb::Transaction<TransactionDB>,
    ) -> Result<(), rocksdb::Error> {
        for indexed_txid in &sum_tx.ins {
            let tx_cache_key = indexed_txid.to_string();
            let utxo_str = db_tx.get(tx_cache_key)?.unwrap();

            let utxo: Utxo = Utxo::try_from(utxo_str).unwrap();
            let address_key = format!(
                "{}|{}|{}|{}",
                utxo.address, "I", indexed_txid.tx_id, indexed_txid.index
            );
            db_tx.put(address_key, utxo.value.to_ne_bytes())?;
        }
        Ok(())
    }

    pub fn get_last_height(&self) -> u64 {
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

    fn store_block_height(
        &self,
        height: u64,
        db_tx: &rocksdb::Transaction<TransactionDB>,
    ) -> Result<(), rocksdb::Error> {
        db_tx.put(LAST_HEIGHT_KEY, height.to_string().as_bytes())?;
        Ok(())
    }

    pub fn update_outputs(&mut self, sum_txs: &Vec<SumTx>) -> Result<(), rocksdb::Error> {
        let db_arc = self.db.clone();
        let db = db_arc.write().unwrap();
        let db_tx = db.transaction();
        for sum_tx in sum_txs {
            self.process_outputs(&sum_tx, &db_tx)?;
        }
        db_tx.commit()?;
        Ok(())
    }

    pub fn update_inputs(
        &mut self,
        height: u64,
        sum_txs: &Vec<SumTx>,
    ) -> Result<(), rocksdb::Error> {
        let db_arc = self.db.clone();
        let db = db_arc.write().unwrap();
        let db_tx = db.transaction();
        for sum_tx in sum_txs {
            if !sum_tx.is_coinbase {
                self.process_inputs(sum_tx, &db_tx)?;
            }
        }
        self.store_block_height(height, &db_tx)?;
        db_tx.commit()?;
        Ok(())
    }
}
