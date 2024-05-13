import multiprocessing
import queue
import book
import time
import pairs
import csv
import traceback

def getProcesses():
    commObjs = []
    for Group in range(len(pairs.assets)):        
        Flag = multiprocessing.Value('i', 0)
        Reciever = multiprocessing.Queue()
        commObjs.append([Flag, Reciever])
        booksetup.append(None)
        b = multiprocessing.Process(target=book.subscribe, 
                                    args=(pairs.assets[Group], Reciever, Flag, Group, 100))
        b.daemon = True
        b.start()
    return  commObjs


if __name__ == '__main__':
    xt = 0           
    st = time.time()
    
    Status = [None]
    booksetup = []
    full = {}
    flag = False
    var = 0

    commObjs = getProcesses()
        
    while xt-st < 300:  ## run for x time
        try:
            xt = time.time()


            # set requestFlag for each process and get data
            # verify times not out of whack 
            # could probably do this in a pool or smth
            if flag:
                flag = False
            else:
                flag = True
                var+=1
            for i in commObjs:
                try:
                    if Status[0]:
                        if flag:
                            i[0].value = 1

                    A = i[1].get(False)
                    if A[1] == 'status':
                        booksetup[A[2]] = A[3]
                    else:
                        full.update(A[1])
                        if xt-A[0] > 1 and xt-st > 15:
                            print(xt-A[0],commObjs.index(i), var)
                except queue.Empty:
                    pass
            
            # review status of processes
            # if one dies, kill all and exit
            
            if booksetup.count(True) == len(pairs.assets):
                if Status[0] == None:
                    print('all status go')
                    Status[0] = True
            else:
                if Status[0] == True:
                    raise Exception('feed dead')
                elif booksetup.count('kill') > 0:
                    raise Exception('feed dead')
                
            time.sleep(0.1)
            
        except Exception as F:
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
    

