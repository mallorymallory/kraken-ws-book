import json
from websocket import create_connection
import time
import traceback
import csv
import binascii

def connect(pairs, Depth):

    for i in range(2):
        try:
            ws = create_connection('wss://ws.kraken.com/')
        except Exception as f:
            print(f)
            time.sleep(1)
        else:
            break
                
    Message = {'event':'subscribe',
                'pair':pairs,
                'subscription':{'name':'book', 'depth':Depth}}
    
    ws.send(json.dumps(Message))
    return ws

        
def disconnect(ws, pairs, depth):    
    ws.close()    
    ws = connect(pairs, depth)
    return ws
     

def checkSum(message, a, b, pair):
    if message != None:
        aa=checkSumHelper(a)
        bb=checkSumHelper(b)
        checkString = ''.join([aa,bb])

        CS = binascii.crc32(checkString.encode('utf-8'))
        
        if CS != int(message):
            print('FAILED', pair, CS, message)
            for i in a:
                print('asks', i)
            for i in b:
                print('bids', i)    
            print(checkString)
            raise Exception('BAD CHECKSUM')


def checkSumHelper(book):
    temp = []
    for lvl in book:
        A = lvl[0]
        A = A.replace('.', '').lstrip('0')
        AA = lvl[1]
        AA = AA.replace('.', '').lstrip('0')
        temp.append(''.join([A,AA]))
    checkString = ''.join(temp)
    return checkString

def timeCheck(now, x):
    if 'r' not in x:
        if now - float(x[2]) > 5:
            return 'BOOK UPDATES TOO SLOW'

def logit(line):
    with open('actionlog.csv', 'a', newline='') as W:
        w = csv.writer(W)
        w.writerow(line)



def subscribe(assetPairs, dataQueue, requestFlag, proID, Depth=10, LogIt=True):
    
    '''
    assetPairs: object denoting TradePairs {"XBT":["USD", "EUR"], ...}
    dataQueue: mp.Queue for passing book data and status updates
    requestFlag: mp.value Int for flagging data requests(1) and kill signals(2) or ignore(0)
    proID: Int, identifies the process ID when logging
    Depth: Depth value to be passed to Kraken WS
    LogIt: WARNING!! logs to CSV lmao

    Safetys:
    1
    * checkSum:  verifies local book copy against Kraken checksum and BOOT if different
    * crossed book checker in Subscribe: BOOT if books crossed
    * BOOT if kraken system status not 'online' (probably could implement some better retry logic here)
    * BOOT if issue unrecognized
    2
    * timeCheck: verifies Kraken vs. User system time and RESET if diff > 5s
    * RESET if subscribes fail due to busy servers
    * RESET if ws connection unexpectedly closes
    
    RESET up to 5 times without success > then BOOT

    full book returned in format: (some processing can be saved by sending reduced book over full)
    {
        "XBT":{
            "USD":{'asks':[sorted data], 'bids':[sorted data]},
            "EUR":{'asks':[sorted data], 'bids':[sorted data]}
            },
        "ETH": ...
    }

    dataQueue messages:
    book:
    [unix timestamp, book]
        *on request
    status:
    [unix timestamp, "status", process ID, <True or 'kill'>]
        *True on receipt of all pair snapshots
        *'kill' on BOOT

    '''

    pairs = []
    retryCounter = 0
    setup = []
    book = {}
 

    for x in assetPairs:
        book.update( { x:{} } )
        for y in assetPairs[x]:
            pairs.append(x + '/'+ y)
            book[x].update( { y:{} } )

    ws = connect(pairs, Depth)
    ws.settimeout(0.1)

    sub = 0
    xxxx = 0
    while True:
        try:
            stst = time.time()
            check = requestFlag.value
            if check == 2:
                raise Exception ('thread killed from handlr')
            elif check == 1:
                xxxx+=1
                dataQueue.put([stst, book, xxxx])
                requestFlag.value = 0
            message = ws.recv()
            message = json.loads(message)   
            if type(message) == dict:
                if message['event'] == 'heartbeat':
                    pass
                elif message['event'] == 'subscriptionStatus':
                    if message['status'] == 'subscribed':
                        sub += 1
                        if sub == len(pairs):
                            if LogIt:
                                logit([stst, proID, 'all subscribes received', ""])

                            retryCounter = 0
                            
                    else:
                        if LogIt:
                            logit([stst, proID, 'subscribe failed', message])
                        raise Exception('subscribe failed'+str(message))
                    
                elif message['event'] == 'systemStatus':
                    if message['status'] == 'online':
                        if LogIt:
                            logit([stst, proID, 'ws connection successful', message])
                    else:
                        if LogIt:
                            logit([stst, proID, 'ws connection failed, ws not online', message])
                        raise Exception('websocket system status is not online')

            if type(message) == list:
                checksum = None
                pair = message[-1]
                base = pair[:pair.index('/')]
                quote = pair[pair.index('/')+1:]
                
                for i in message:
                    if type(i) == dict:
                        if 'as' in list(i.keys()):
                            book[base][quote].update({'asks':i['as'],'bids':i['bs']})
                            setup.append(pair)
                            if len(setup) == len(pairs):
                                dataQueue.put([stst, 'status', proID, True])

                            
                            
                        if 'a' in list(i.keys()) or 'b' in list(i.keys()):
                            side = ('a' if 'a' in list(i.keys()) else 'b')                        
                            lside = ('bids' if side == 'b' else 'asks')
                                
                            for lvl in i[side]:
                                found = False
                                for blvl in book[base][quote][lside]:
                                    if float(blvl[0]) == float(lvl[0]):
                                        if float(lvl[1]) == 0:
                                            book[base][quote][lside].pop(book[base][quote][lside].index(blvl))
                                        else:
                                            book[base][quote][lside][book[base][quote][lside].index(blvl)] = lvl
                                        found = True
                                        break
                                if found == False and float(lvl[1]) != 0:
                                    book[base][quote][lside].append(lvl)

                                health = timeCheck(stst, lvl)
                                if health != None:
                                    raise Exception('book invalid too long')
                            
                            book[base][quote][lside].sort(reverse= True if lside == 'bids' else False, key=lambda x: float(x[0]) )
                            
                            while len(book[base][quote][lside]) > Depth:
                                book[base][quote][lside].pop()
                            
                        if 'c' in list(i.keys()):
                            checksum = i['c']
                            checkSum(checksum, 
                                book[base][quote]['asks'][0:10],  
                                book[base][quote]['bids'][0:10], 
                                pair)        
                
                if float(book[base][quote]['asks'][0][0]) < float(book[base][quote]['bids'][0][0]):
                    raise Exception ("CROSSED BOOKS ON:", pair)

        except Exception as F:
            if str(F) in ['book invalid too long', 'Connection is already closed.',
                            "subscribe failed{'errorMessage': 'EService:Busy', 'event': 'subscriptionStatus', 'status': 'error', 'subscription': {'name': 'book'}}"]:
                retryCounter += 1
                if retryCounter == 6:
                    if LogIt:
                        logit([stst, proID, 'Book WS Failed:Too many retries:' + str(F), traceback.format_exc()])

                    print(F)
                    dataQueue.put([stst, 'status', proID, 'kill'])
                    break
                else:
                    if LogIt:
                        logit([stst, proID, 'Book WS Issue:' + str(F) + ' retrying', traceback.format_exc(), message])
                    ws = disconnect(ws, pairs, Depth)
                    ws.settimeout(0.1)
                    setup = []
                    sub = 0

            elif str(F) in ['The read operation timed out']:
                pass
            else:
                if LogIt:
                    logit([stst, proID, 'Book WS Failed:' + str(F), traceback.format_exc(), message])
                print([str(F)])
                dataQueue.put([stst, 'status', proID, 'kill'])
                break
    return