import multiprocessing 
import os
import time 
import random

def table_scan(code, start, end, engine):
    wget_command = "wget -q -O - http://pzque.com/test/query/scan\?code\=" + str(code) + "\&start\=" + str(start) + "\&end\=" + str(end) + "\&engine\=" + str(engine)
    os.system(wget_command)
    return 0


def table_get(code, timestamp, engine):
    wget_command = "wget -q -O - http://pzque.com/test/query/get\?code\=" + str(code) + "\&timestamp\=" + str(timestamp) + "\&engine\=" + str(engine)
    os.system(wget_command)
    return 0


pool = multiprocessing.Pool(processes = 10)
method = 'get'
concur = 100
date = ['20180103','20180104','20180105','20180107','20180108','20180109']
engine = 'hbase'

def rand_suffix():
    suffix = '' 
    return suffix

start = time.time()
if method == 'scan':
    for i in range(concur):
        code = '000011' 
        start = str(date[random.randint(0, len(date) - 1)]) + '150006'
        end = str(date[random.randint(0, len(date) - 1)]) + '150006'
        pool.apply_async(table_scan, (code, start, end, engine,))
    pool.close()
    pool.join()
else:
    for i in range(concur):
        code = '000011'
        timestamp = str(date[random.randint(0, len(date)-1)]) + '150006'
        pool.apply_async(table_get, (code, timestamp, engine,))
    pool.close()
    pool.join()

end = time.time()

print 'process time %.5f'%(end - start)
