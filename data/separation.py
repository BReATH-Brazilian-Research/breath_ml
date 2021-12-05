import multiprocessing as mp
import sqlite3 as sql
import time

import pandas as pd
import numpy as np
from geopy.distance import distance as geodist

def test_func(args):
    info_queue :mp.Queue = args[0]
    index = args[1]
    info_queue.put("Starting "+str(index))
    time.sleep(args[2])
    info_queue.put("Ending "+str(index))


"SELECT Count(*) FROM SRAG WHERE SRAG.ID_MUNICIP = '{0}'  AND ()"

if __name__ == "__main__":

    manager = mp.Manager()
    pool = mp.Pool()

    info_queue = manager.Queue()

    args = [[info_queue, i, 1] for i in range(24)]
    # [[info_queue, 0, 0], [info_queue, 1, 1], [info_queue, 2, 2]]

    map_result = pool.map_async(test_func, args)

    print("Teste")

    while(True):
        print(info_queue.get())
        

        if map_result.ready() == True:
            break

    pool.close()
