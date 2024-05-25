## indexBTC

Indexes whole Bitcoin ledger for real-time address balance access.

### Run 

1. For testing purposes, install `bitcoind` on a beefy machine with setting `rpcworkqueue=32` and `rpcthreads=32` for eager syncing up to at least 1M height
2. Restat `bitcoind` with setting `-maxconnections=0` so it stops syncing
3. Start `indexBTC` and let it sync with your existing chain

```
export BITCOIN_RPC_USERNAME=foo
export BITCOIN_RPC_PASSWORD=bar

cargo run http://127.0.0.1:8332
```