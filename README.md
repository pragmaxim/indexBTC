## indexBTC

Indexes whole Bitcoin ledger for real-time address balance access.
Uses either [RocksDB](https://rocksdb.org/) or [Sled](https://github.com/spacejam/sled/), which performs better on beefy machines.

### Run 

1. For testing purposes, install `bitcoind` on a beefy machine with setting `rpcworkqueue=32` and `rpcthreads=64` for eager syncing up to at least 1M height
2. Restat `bitcoind` with setting `-maxconnections=0` so it stops syncing
3. Start `indexBTC` and let it sync with your existing chain

```
$./index_btc --help
Bitcoin transactions indexer

Usage: index_btc [OPTIONS]

Options:
      --db-path=<db-path>      Absolute path to db directory [default: /tmp/index_btc]
      --btc-url=<btc-url>      Url of local bitcoin-core [default: http://127.0.0.1:8332]
      --db-engine=<db-engine>  rocks-db or sled-db [default: rocks-db]
  -h, --help                   Print help
  -V, --version                Print version
```