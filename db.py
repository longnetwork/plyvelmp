#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os, logging, threading

from ast import literal_eval

from contextlib import contextmanager

# from leveldb import LevelDB
from plyvel import DB as LevelDB


class _DB:  # LevelDB не дается наследоваться
    """
        Ключи - всегда строки (с возможностью итерации по префиксу в лексикографическом порядке)
        Значения - Любые (хешируемые) python-объекты

        XXX Инстанс базы должен быть один для данного path, и шарится глобально для всего приложения
    """

    SALT = __qualname__ + 'IQyydZVKfILFCfcd'


    
    BLOCK_SIZE = 16 * 1024
    WRITE_BUFFER_SIZE = BLOCK_SIZE * 1024 * 16

    PARANOID_CHECKS = True; VERIFY_CHECKSUMS = True;

    FILL_CACHE = True;  # При массовом сканировании чтобы избежать расхода памяти опция должна быть False (True - для ускорения повторного чтения)

    SYNC = False;       # True - медленный режим со "скидыванием" дискового кеша после каждой операции записи (важен только при отрубании питания)

    # ~ if os.name == 'nt':
        # ~ COMPRESSION: "None | 'snappy'" = None;  # FIXME Под Windows 'snappy' приводит к краху python3.11 (snappy на тестах ужимает в 2 раза)
    # ~ else:
        # ~ COMPRESSION: "None | 'snappy'" = 'snappy'

    COMPRESSION: "None | 'snappy'" = 'snappy'

    def __init__(self, path):

        self.path = path
        
        self._db = None
        self._db = LevelDB(path, create_if_missing=True, compression = self.COMPRESSION,

                           paranoid_checks=self.PARANOID_CHECKS,
                           write_buffer_size=self.WRITE_BUFFER_SIZE,
                           block_size=self.BLOCK_SIZE)
        
        self.tlock = threading.RLock();  # Блокировка уровня экземпляра класса

        logging.info(f"DB {self.path}: Opened")
        
        
    def __close(self):
        if hasattr(self, '_db') and self._db: self._db.close()

    def __del__(self):
        """
            Это расшаренный между потоками объект, поэтому внезапно закрыть
            в одном потоке и "обломить" другие мы не можем (close не предоставляется)
        """
        self.__close()
        '''
        try:
            # FIXME при выходе логер для logfile уже может не существовать из-за gc:
            logging.info(f"DB {self.path}: Closed")
        except Exception:
            print(f"DB {self.path}: Closed")
        '''


    def get(self, key):
        if not isinstance(key, str):
            raise AssertionError(f"DB {self.path}: key must be a string")
            
        with self.tlock:
            val = self._db.get(key.encode(), verify_checksums=self.VERIFY_CHECKSUMS, fill_cache=self.FILL_CACHE)
            
        if val is None: return None

        try:
            return literal_eval(val.decode())
        except Exception:
            raise RuntimeError(f"DB {self.path}: Invalid Value {val} for key {key}") from None


    def put(self, key, val):
        if not isinstance(key, str):
            raise AssertionError(f"DB {self.path}: key must be a string")
        
        val = repr(val).encode()

        with self.tlock:
            self._db.put(key.encode(), val, sync=self.SYNC)


    def delete(self, key):
        if not isinstance(key, str):
            raise AssertionError(f"DB {self.path}: key must be a string")
        with self.tlock:
            self._db.delete(key.encode(), sync=self.SYNC)


    def iterator(self, prefix=None, reverse=False, *, seek: "part after prefix" = None):
        prefix = prefix or ''
        
        if not isinstance(prefix, str):
            raise AssertionError(f"DB {self.path}: Iterator prefix must be a string")

        if seek is not None:
            if not isinstance(seek, str):
                raise AssertionError(f"DB {self.path}: Iterator seek must be a string")
        

        self.tlock.acquire();   # При первом next()

        it = None
        
        try:
            
            with self._db.iterator(prefix=prefix.encode(), reverse=reverse,  verify_checksums=self.VERIFY_CHECKSUMS, fill_cache=self.FILL_CACHE) as it:

                if seek is not None:
                    # seek может выходить за рамки префикса и давать первый next вне префикса
                    it.seek((prefix + seek).encode());  # XXX seek() всегда без исключений. 

                prefix_len = len(prefix); prefix_enc = prefix.encode()
                
                for key, val in it:
                    
                    self.tlock.release()
                    
                    try:
                        if not key.startswith(prefix_enc):          # XXX seek может проскакивать позицию префикса
                            continue
                        
                        if val is None:
                            yield key.decode()[prefix_len:], None;  # Только суффикс ключа (prefix - имя таблицы)
                        else:

                            try:
                                yield key.decode()[prefix_len:], literal_eval(val.decode())
                            except Exception:
                                raise RuntimeError(f"DB {self.path}: Invalid Value {val} for key {key}") from None
                            
                    finally:    # Либо блокировка на следующую итерацию либо пара для конечного finally
                        self.tlock.acquire()
                       
        finally:                # При raise StopIteration / iterator().close()
            
            if it: it.close()
                
            self.tlock.release()


    
    def __enter__(self):                                 # Менеджер контекста для последовательных синхронизированных операций put/get с оператором with
        self.tlock.acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback):  # Возвращает None - само исключение обрабатывается во вне
        self.tlock.release()


    @contextmanager
    def write_batch(self):
        """
            Последовательная запись с откатом если в середине исключение
        """
        
        class WriteBatch:
            """
                Методы записи для инстанца write_batch не имеют параметра sync (он указывается при создании инстанца)
            """
            def __init__(self, owner, wb):

                self.path = owner.path
                
                self.wb = wb

                self.tlock = owner.tlock

            def put(self, key, val):
                if not isinstance(key, str):
                    raise AssertionError(f"WB {self.path}: key must be a string")
                
                val = repr(val).encode()

                with self.tlock:
                    self.wb.put(key.encode(), val)

            def delete(self, key):
                if not isinstance(key, str):
                    raise AssertionError(f"DB {self.path}: key must be a string")

                with self.tlock:
                    self.wb.delete(key.encode())
            
        
        with self._db.write_batch(transaction=True, sync=self.SYNC) as wb:
            try:
                yield WriteBatch(self, wb);  # self == owner
            except GeneratorExit:  # XXX Менеджер контекста plyvel воспринимает это как исключение и сбрасывает транзакции write_batch
                pass               # Это может быть если контекст write_batch() используется внутри генератора/итератора 



    def get_stats(self):
        if hasattr(self, '_db') and self._db:
            return self._db.get_property(b'leveldb.stats').decode()
        else:
            return None


class DB(_DB):
    """
        Singleton Базы для юзания в разных частях кода глобально (только потоковая безопасность)
    """

    db_instances = {};  # Хранит ссылку на базу (закрытие естественным путем через __del__ при выходе из приложения)

    tlock = threading.RLock()

    
    def __new__(cls, path='DB'):
        with cls.tlock:
            if path not in cls.db_instances:
                self = super().__new__(cls); super(cls, self).__init__(path)
                cls.db_instances[path] = ( self, );  # Если без исключений то синглетон создан
            else:
                self, *_ = cls.db_instances[path]
            
            return self
    
    def __init__(self, path='DB'):  # pylint: disable=W0231
        """
            Вся инициализация в __new__() здесь только проверка аргументов
        """
        cls = type(self)

        with cls.tlock:
            _self, *_ = cls.db_instances[path]                
            if _self != self:
                raise RuntimeError(f"DB {path}: is Already Open")



