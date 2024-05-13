# kraken-ws-book
a python multiprocessing module to build and access kraken orderbooks in real time. 


Pre-Requisite: python3 websocket-client 
https://github.com/websocket-client/websocket-client

Usage:
pass pair object to subscribe function:
< {"XBT":["USD", "EUR"], "ETH":["USD", "EUR"]...} >
returns completed book via mp.Queue on request from mp.Value flag (1):
{
    "XBT":{
        "USD":{'asks':[sorted data], 'bids':[sorted data]},
        "EUR":{'asks':[sorted data], 'bids':[sorted data]}
        },
    "ETH": ...
}

Book processing takes ~0.0001s, and sending data takes a similar time (depending on depth/amount of data) -- ~5000 msgs/sec processing possible depdning on machine (so keep in mind when dictacting pairs/process) - 

for additional speed saftey checks like checksum can be disabled, and concentrated book data could be sent 
