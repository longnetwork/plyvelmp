#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, textwrap

from multiprocessing import shared_memory, util, resource_tracker


"""
    FIXME В python3.11 еще нету опции track=False при создании SharedMemory.

    python3.11/multiprocessing/resource_tracker.py#main запускается в отдельном инстанце питона:
        cmd = 'from multiprocessing.resource_tracker import main;main(%d)'
    поэтому, чтобы убрать лишний выхлоп в консоль мы можем только изменить момент запуска в ensure_running:
        pid = util.spawnv_passfds(exe, args, fds_to_pass)

    XXX Этот фикс нужен только для Linux (в винде все ОК)
"""

if os.name == 'posix':
    _spawnv_passfds = util.spawnv_passfds
    
    def spawnv_passfds(path, args, passfds):
        # path: b'/home/mint/Work/plyvel-mp/.venv/bin/python'
        # args: [b'/home/mint/Work/plyvel-mp/.venv/bin/python', '-c', 'from multiprocessing.resource_tracker import main;main(10)']
        # passfds: [2, 10]

        cmd = args[-1]
        if 'resource_tracker' in cmd:
            cmd = textwrap.dedent(f"""
                import sys
                sys.excepthook = lambda *args: None;  # Подавляем не нужный нам выхлоп. FIXME лучше с фильтрацией по SharedMemory
                {cmd}
            """)
            
            cmd = f'exec("""{cmd}""")'
            
            args[-1] = cmd
        
        return _spawnv_passfds(path, args, passfds)

    util.spawnv_passfds = spawnv_passfds


class SharedMemory(shared_memory.SharedMemory):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if os.name == 'posix':
            resource_tracker.unregister(self._name, 'shared_memory')
            


            
            
