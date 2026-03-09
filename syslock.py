#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys

import struct
from time import sleep

from multiprocessing import shared_memory, Lock


class SysLock:
    """
        Кросплатформенная альтернатива именованным мьютексам и файловым блокировкам

        XXX НЕ рекурсивная
    """

    SALT = __qualname__ + 'hTRxcJTsFYsMNsLg'
    
    TICK = sys.getswitchinterval() / 3

    SIZE = struct.calcsize("P")


    def __init__(self, name=''):
        self.shm = None
        self.name = SysLock.SALT + name
        self.lock = Lock()
        
    def acquire(self):
        while True:
            try:
                with self.lock:
                    self.shm = shared_memory.SharedMemory(name= self.name, create=True, size= SysLock.SIZE);  # 50us ~ 5ms
                break
            except FileExistsError:
                sleep(SysLock.TICK)

        return True
        
    def release(self):
        with self.lock:
            shm, self.shm = self.shm, None
            
            if not shm:
                raise RuntimeError("release unlocked syslock");  # ValueError: semaphore or lock released too many times
            
            shm.close()
            shm.unlink()


    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()
    
