#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import struct
from multiprocessing import shared_memory
from time import sleep

class SysLock:
    """
        Кросплатформенная альтернатива именованным мьютексам и файловым блокировкам

        XXX НЕ рекурсивная
    """

    SALT = __qualname__ + 'hTRxcJTsFYzMNsLd'
    
    TICK = 0.00001

    SIZE = struct.calcsize("P")


    def __init__(self, name=''):
        self.shm = None
        self.name = SysLock.SALT + name
        
    def acquire(self):
        while True:
            try:
                self.shm = shared_memory.SharedMemory(name= self.name, create=True, size= SysLock.SIZE);  # 50us ~ 5ms
                break
            except FileExistsError:
                sleep(SysLock.TICK)

        return True
        
    def release(self):
        if not self.shm:
            raise RuntimeError("release unlocked syslock");  # ValueError: semaphore or lock released too many times

        self.shm.close()
        self.shm.unlink()
        self.shm = None


    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()
    
