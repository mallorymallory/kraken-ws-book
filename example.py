import multiprocessing
import queue
import book
import time
import pairs
import csv
import traceback

if __name__ == '__main__':
    xt = 0           
    st = time.time()

    Status = [None]
    booksetup = []
    full = {}
    flag = False
    var = 0

    commObjs = []
    for Group in range(len(pairs.assets)):        
        Flag = multiprocessing.Value('i', 0)
        Reciever = multiprocessing.Queue()
        commObjs.append([Flag, Reciever])
        booksetup.append(None)
        b = multiprocessing.Process(target=book.subscribe, 
                                    args=(pairs.assets[Group], Reciever, Flag, Group, 500))
        b.daemon = True
        b.start()

    while Status[0] == None:   ## wait for all status updates to be received before polling data
        for q in commObjs:
            try:
                status = q[1].get(False)
                if status[1] == 'status':
                    booksetup[status[2]] = status[3]
                    if status[3] == 'kill':
                        raise Exception('MAIN: kill signal recved during startup')
                else:
                    raise Exception('MAIN: unexpected respnse received during startup')
            except queue.Empty:
                pass

        if booksetup.count(True) == len(booksetup):        
            print('all status go')
            Status[0] = True
            break
        time.sleep(0.1)


    while xt-st < 7200:  ## run for x time

        try:

            xt = time.time()
            if int(xt-st) % 60 == 0:
                print(int(int(xt-st)/60), ':)', full['XBT']['USD']['bids'][0], '>', full['XBT']['USD']['asks'][0])


            # set requestFlag for each process and get data
            # verify booktime v maintime not out of whack 
            
            ff = time.perf_counter()    
            ## if flag was set true last round > get data, else: nothing
            for i in commObjs:
                try:
                    A = i[1].get(flag, 5 if flag else None)
                    
                    if A[1] == 'status':
                        if A[3] == 'kill':
                            raise Exception('feed died')
                    else:
                        full.update(A[1])
                        #if A[2] != var:
                        if (xt-A[0] > 1 and xt-st > 15) or A[2] != var: # timer check and refid check
                            print(xt, A[0],)
                            print(xt-A[0],commObjs.index(i), A[2], var) ### timer warning
                except queue.Empty:
                    if flag:
                        raise Exception('timed out on message '+str(var)+' from thread '+str(commObjs.index(i)))
                    else:
                        pass
            ff2 = time.perf_counter()
            #print(ff2-ff)
            if ff2-ff > 0.5:
                print('concerning..')
            
            ##Set flag before sleep to reduce wait during .get above
            
            if flag:
                flag = False
            else:
                flag = True
                var+=1
                for i in commObjs:
                    i[0].value = 1
                
            time.sleep(0.2)
            
            
        except Exception as F:
            if str(F) == 'list index out of range':
                print(full['XBT']['USD'])
            print(F)
            if str(F) in ['feed dead', 'killed from main']:
                with open('actionlog.csv', 'a', newline='') as err:
                    eeee = csv.writer(err)
                    eeee.writerow([time.time(), 'bookhandlr', 'check alive found ded', 'books: '+str(booksetup)])
                              
            else:
        
                with open('actionlog.csv', 'a', newline='') as err:
                    eeee = csv.writer(err)
                    eeee.writerow([time.time(), 'bookhandlr', str(F), str(traceback.format_exc()), 'books: '+str(booksetup)])
                         
            for i in commObjs:
                i[0].value = 2
            
            Status[0] = 'kill'
            print('killcall sent and exiting', Status)
            break

    print('trying to exit now')   
    for i in commObjs:
        i[0].value = 2
    print('killcall sent and exiting', Status)
    

