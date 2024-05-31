use index_btc::indexer::{Indexer, IndexerError};
use index_btc::model::{self, SumTx, Utxo, ADDRESS_CF, CACHE_CF, LAST_HEIGHT_KEY, META_CF};
use sled::transaction::{TransactionError, Transactional, UnabortableTransactionError};
use sled::Tree;
use std::sync::{Arc, RwLock};

pub struct SledDbIndexer {
    db: Arc<RwLock<sled::Db>>,
}

impl Clone for SledDbIndexer {
    fn clone(&self) -> SledDbIndexer {
        SledDbIndexer {
            db: Arc::clone(&self.db),
        }
    }
}

impl SledDbIndexer {
    // Method to process the outputs of a transaction
    fn process_outputs(
        &self,
        sum_tx: &SumTx,
        tree: &sled::transaction::TransactionalTree,
        batch: &mut sled::Batch,
    ) -> Result<(), UnabortableTransactionError> {
        for utxo in sum_tx.outs.iter() {
            let tx_id_with_index = format!("{}|{}", &sum_tx.txid, utxo.index);
            let utxo_bytes = utxo.to_string().into_bytes();
            tree.insert(tx_id_with_index.as_bytes(), utxo_bytes)?;
            let address_key = format!("{}|{}|{}|{}", utxo.address, "O", &sum_tx.txid, utxo.index);
            batch.insert(
                address_key.as_bytes(),
                u64::to_be_bytes(utxo.value).as_slice(),
            );
        }
        Ok(())
    }

    // Method to process the inputs of a transaction
    fn process_inputs(
        &self,
        sum_tx: &SumTx,
        tree: &sled::transaction::TransactionalTree,
        batch: &mut sled::Batch,
    ) {
        for indexed_txid in &sum_tx.ins {
            let tx_cache_key = indexed_txid.to_string();
            let utxo_str = tree.get(tx_cache_key).unwrap().unwrap();
            let utxo: Utxo = Utxo::try_from(utxo_str.to_vec()).unwrap();
            let address_key = format!(
                "{}|{}|{}|{}",
                utxo.address, "I", indexed_txid.tx_id, indexed_txid.index
            );
            batch.insert(
                address_key.as_bytes(),
                u64::to_be_bytes(utxo.value).as_slice(),
            );
        }
    }
}

impl Indexer for SledDbIndexer {
    fn update_balance(
        &mut self,
        height: u64,
        sum_txs: &Vec<model::SumTx>,
    ) -> Result<(), IndexerError> {
        let db_arc = self.db.clone();
        let db = db_arc.write().unwrap();
        let address_tree = db.open_tree(ADDRESS_CF).unwrap();
        let cache_tree: Tree = db.open_tree(CACHE_CF).unwrap();
        let meta_tree: Tree = db.open_tree(META_CF).unwrap();

        (&address_tree, &cache_tree, &meta_tree)
            .transaction(|(address_tree, cache_tree, meta_tree)| {
                let mut address_batch = sled::Batch::default();
                for sum_tx in sum_txs {
                    self.process_outputs(&sum_tx, &cache_tree, &mut address_batch)?;
                    if !sum_tx.is_coinbase {
                        self.process_inputs(sum_tx, &cache_tree, &mut address_batch);
                    }
                }
                address_tree.apply_batch(&address_batch).unwrap();
                meta_tree.insert(LAST_HEIGHT_KEY, height.to_string().as_bytes())?;
                Ok(())
            })
            .map_err(|e: TransactionError| IndexerError::SledError(e.to_string()))?;
        Ok(())
    }

    fn get_last_height(&self) -> u64 {
        use zerocopy::FromBytes;

        return self
            .db
            .clone()
            .read()
            .unwrap()
            .get(LAST_HEIGHT_KEY)
            .unwrap()
            .map_or(0, |height| u64::read_from(height.as_ref()).unwrap());
    }

    fn new(num_cores: i32, db_path: &str) -> Result<Self, IndexerError> {
        let instance: sled::Db =
            sled::open(db_path).map_err(|e| IndexerError::SledError(e.to_string()))?;
        Ok(SledDbIndexer {
            db: Arc::new(RwLock::new(instance)),
        })
    }
}
