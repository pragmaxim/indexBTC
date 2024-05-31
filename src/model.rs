use bitcoin::{Address, Network, Transaction};
use sha2::{Digest, Sha256};
use std::num::ParseIntError;
use std::str::FromStr;
use std::string::FromUtf8Error;
use std::{fmt, str};

pub const OP_RETURN: &str = "OP_RETURN";
pub const LAST_HEIGHT_KEY: &[u8] = b"last_height";

pub const ADDRESS_CF: &str = "ADDRESS_CF";
pub const CACHE_CF: &str = "CACHE_CF";
pub const META_CF: &str = "META_CF";

#[derive(Debug)]
pub enum Flow {
    I,
    O,
}

#[derive(Debug)]
pub struct AddressFlow {
    pub address: String,
    pub flow: Flow,
    pub tx_id: String,
    pub utxo_index: usize,
}

impl fmt::Display for AddressFlow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}",
            self.address, self.flow, self.tx_id, self.utxo_index
        )
    }
}

impl fmt::Display for Flow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Flow::I => write!(f, "I"),
            Flow::O => write!(f, "O"),
        }
    }
}

impl FromStr for Flow {
    type Err = UtxoParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // parse flow from string
        match s {
            "I" => Ok(Flow::I),
            "O" => Ok(Flow::O),
            _ => Err(UtxoParseError::InvalidFormat(format!(
                "Invalid flow : {}",
                s
            ))),
        }
    }
}

#[derive(Debug)]
pub enum UtxoParseError {
    DecodingError(FromUtf8Error),
    ParseInt(ParseIntError),
    InvalidFormat(String),
}

impl TryFrom<Vec<u8>> for AddressFlow {
    type Error = UtxoParseError;

    fn try_from(utxo_str: Vec<u8>) -> Result<Self, Self::Error> {
        let utxo = String::from_utf8(utxo_str)
            .map_err(|err| UtxoParseError::DecodingError(err))?
            .parse()?;
        Ok(utxo)
    }
}

impl FromStr for AddressFlow {
    type Err = UtxoParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('|').collect();
        if parts.len() != 4 {
            return Err(UtxoParseError::InvalidFormat(format!(
                "Invalid Address : {}",
                s
            )));
        }

        let address = parts[0].to_string();
        let flow: Flow = parts[1].parse()?;
        let tx_id = parts[2].to_string();
        let utxo_index = parts[3]
            .parse::<usize>()
            .map_err(|err| UtxoParseError::ParseInt(err))?;

        Ok(AddressFlow {
            address,
            flow,
            tx_id,
            utxo_index,
        })
    }
}

#[derive(Debug, Clone)]
pub struct SumTx {
    pub is_coinbase: bool,
    pub txid: String,
    pub ins: Vec<IndexedTxid>,
    pub outs: Vec<Utxo>,
}

impl From<Transaction> for SumTx {
    fn from(tx: Transaction) -> Self {
        SumTx {
            is_coinbase: tx.is_coinbase(),
            txid: tx.compute_txid().to_string(),
            ins: tx
                .input
                .iter()
                .map(|input| IndexedTxid {
                    index: input.previous_output.vout as usize,
                    tx_id: input.previous_output.txid.to_string(),
                })
                .collect(),
            outs: tx
                .output
                .iter()
                .enumerate()
                .map(|(out_index, out)| {
                    let address = if let Ok(address) =
                        Address::from_script(out.script_pubkey.as_script(), Network::Bitcoin)
                    {
                        address.to_string()
                    } else if let Some(pk) = out.script_pubkey.p2pk_public_key() {
                        bitcoin::Address::p2pkh(pk.pubkey_hash(), bitcoin::Network::Bitcoin)
                            .to_string()
                    } else if out.script_pubkey.is_op_return() {
                        OP_RETURN.to_string()
                    } else {
                        let mut hasher = Sha256::default();
                        hasher.update(&out.script_pubkey.to_string());
                        base16::encode_lower(&hasher.finalize())
                    };
                    Utxo {
                        index: out_index,
                        address: address.to_string(),
                        value: out.value.to_sat(),
                    }
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndexedTxid {
    pub tx_id: String,
    pub index: usize,
}

impl fmt::Display for IndexedTxid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}|{}", self.tx_id, self.index)
    }
}

impl FromStr for IndexedTxid {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('|').collect();
        if parts.len() != 2 {
            return Err("Invalid format");
        }

        let index = parts[0].parse::<usize>().map_err(|_| "Invalid value")?;
        let tx_id = parts[1].to_string();
        Ok(IndexedTxid { tx_id, index })
    }
}

#[derive(Debug, Clone)]
pub struct Utxo {
    pub index: usize,
    pub address: String,
    pub value: u64,
}

impl fmt::Display for Utxo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}|{}|{}", self.index, self.address, self.value)
    }
}

impl TryFrom<Vec<u8>> for Utxo {
    type Error = UtxoParseError;

    fn try_from(utxo_str: Vec<u8>) -> Result<Self, Self::Error> {
        let utxo = String::from_utf8(utxo_str)
            .map_err(|err| UtxoParseError::DecodingError(err))?
            .parse()?;
        Ok(utxo)
    }
}

impl FromStr for Utxo {
    type Err = UtxoParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('|').collect();
        if parts.len() != 3 {
            return Err(UtxoParseError::InvalidFormat(format!(
                "Invalid UTXO : {}",
                s
            )));
        }

        let utxo_index = parts[0]
            .parse::<usize>()
            .map_err(|err| UtxoParseError::ParseInt(err))?;
        let address = parts[1].to_string();
        let value = parts[2]
            .parse::<u64>()
            .map_err(|err| UtxoParseError::ParseInt(err))?;

        Ok(Utxo {
            index: utxo_index,
            address,
            value,
        })
    }
}
