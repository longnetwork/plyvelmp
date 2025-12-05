#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from multiprocessing import shared_memory

class SharedMemory(shared_memory.SharedMemory):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
            


            
            
