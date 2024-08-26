Asynchronous script for parsing Bitcoin blockchain transactions.
Асинхронный скрипт парсинга транзакций блокчейна биткоина.


bitcoincore config 

rpcthreads=8
rpcworkqueue=10
debug=http

zmqpubhashblock=tcp://127.0.0.1:28332
zmqpubhashtx=tcp://127.0.0.1:28333
zmqpubrawblock=tcp://127.0.0.1:28334
zmqpubrawtx=tcp://127.0.0.1:28335


dbcache=6000
maxmempool=5000
par=12



maxconnections=3
listen=0
upnp=0
natpmp=0
dnsseed=0

disablewallet=1
blocksonly=1

txindex=1

