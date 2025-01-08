#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import logging, os
from multiprocessing import current_process, parent_process, Process, RLock

from time import sleep

from ast import literal_eval

from .db import DB

from .syslock import SysLock
from .shm import SharedMemory


def maintainer(**kwargs):  # Для spawn - target процесса через жопу Била Гейтса
    mdb_cls = globals()['MDB']; _maintainer = getattr(mdb_cls, '_MDB__maintainer'); _maintainer(**kwargs)


class MDB:
    """
        Для поддержки многопроцессного доступа к levelDB
        (может быть только одно подключение на все приложение к одной и той-же BD)

        Мы создаем рабочий процесс, который монопольно общается с levelDB. Взаимодействие с
        другими процессами - через SharedMemory

        Это Singleton - он либо создает SharedMemory либо подключается к существующей

        daemon=True - Безусловный "дроп" субпроцесса если родительский завершился
        daemon=False - Продолжает "курится" до завершения (родитель зависает)

        XXX:
            Action	                                                        fork spawn
            
            Create new PID for processes	                                yes	 yes
            Module-level variables and functions present	                yes	 yes
            Child processes independently track variable state	            yes	 yes
            Import module at start of each child process	                no	 yes
            Variables have same id as in parent process	                    yes	 no
            Child process gets variables defined in name == main block	    yes	 no
            Parent process variables are updated from child process state   no   no
            Threads from parent process run in child processes	            no   no
            Threads from parent process modify child variables	            no   no

        XXX https://github.com/google/leveldb/blob/main/doc/index.md:
            A database may only be opened by one process at a time. The leveldb implementation acquires a lock
            from the operating system to prevent misuse. Within a single process, the same leveldb::DB object may
            be safely shared by multiple concurrent threads. I.e., different threads may write into or fetch iterators or
            call Get on the same database without any external synchronization (the leveldb implementation will automatically
            do the required synchronization). However other objects (like Iterator and WriteBatch) may require external synchronization.
            If two threads share such an object, they must protect access to it using their own locking protocol.
            More details are available in the public header files.


        Формат SharedMemory:
            [ lock0, lock1, ...,    state0, state1, ...,    data0, data1, ... ],
            где lock  - флаги захваченных процессами слотов;
                state - байт состояния для синхронизации: 0 - idle, 1 - request, 2 - responce;
                data  - данные ввода/вывода в литеральных b-строках (образованных от словаря);
    """

    BLOCK_SIZE = DB.BLOCK_SIZE;  # Определяет максимальное число байт в data
    # SALT = DB.SALT
    SALT = __qualname__ + 'muqpjaTWTcwHmmqL';  # alphabet = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'

    assert BLOCK_SIZE >= 512

    # Мы можем подключаться из другой программы, где объекты синхронизации не доступны (работаем в режиме software lock)
    TICK = SysLock.TICK = 0.00001

    assert TICK > 0.0000001


    MAX_PROCESSES = 24;  # Определяет размер SharedMemory для обслуживания процессов без очереди

    
    STATE_IDLE, STATE_REQUEST, STATE_RESPONCE = range(3);  # XXX b'\0'[0] == 0, b'\1'[0] == 1, ...

    LOCK_FREE, LOCK_LOCK, LOCK_CLEAN = range(3);           # False, True, 2

    @staticmethod
    def __seek_lock(index): return (index % MDB.MAX_PROCESSES)
    
    @staticmethod
    def __seek_state(index): return MDB.MAX_PROCESSES + (index % MDB.MAX_PROCESSES)
            
    @staticmethod
    def __seek_data(index): return MDB.MAX_PROCESSES + MDB.MAX_PROCESSES + (index % MDB.MAX_PROCESSES) * MDB.BLOCK_SIZE


    @staticmethod
    def __put_data(shm_buf, index, data):
        seek = MDB.__seek_data(index)
        
        raw = repr(data).encode() + b'\0'

        if len(raw) > MDB.BLOCK_SIZE:
            raise BufferError
        
        shm_buf[seek: seek + len(raw)] = raw

    @staticmethod
    def __get_data(shm_buf, index):
        seek = MDB.__seek_data(index)
        
        raw = bytes(shm_buf[seek: seek + MDB.BLOCK_SIZE])

        try:
            end = raw.index(b'\0')
        except ValueError:
            raise BufferError from None

        return literal_eval(raw[0: end].decode())

    @staticmethod
    def __get_tasks(shm_buf):
        return list(shm_buf[MDB.__seek_state(0): MDB.__seek_state(MDB.MAX_PROCESSES - 1) + 1])

    @staticmethod
    def __get_locks(shm_buf):
        return list(shm_buf[MDB.__seek_lock(0): MDB.__seek_lock(MDB.MAX_PROCESSES - 1) + 1])

    @staticmethod
    def __clr_locks(shm_buf):
        shm_buf[MDB.__seek_lock(0): MDB.__seek_lock(MDB.MAX_PROCESSES - 1) + 1] = bytes([False] * MDB.MAX_PROCESSES)
            


    __maintainer_proc = None

        
    def __init__(self, path='DB'):
        
        self.path = path

        self.salt = MDB.SALT + path.replace(os.path.pathsep, '').replace(os.path.sep, '')

        self.shm = None; self.index = -1

        self.plock = RLock();  # Для защиты о неверного использования (одного сокета подключения в разных субпроцессах)

        with SysLock(self.salt):
            try:
                # Аттач к существующей SharedMemory
                self.shm = SharedMemory(name=self.salt, create=False)
                logging.info(f"Attach SharedMemory {self.shm.name}, process {current_process().name}")
                
            except FileNotFoundError:
                # Обслуживающий процесс еще не запущен - запускаем и подключаемся к SharedMemory

                if parent_process():
                    raise RuntimeError("Only Main Process must create SharedMemory") from None
                
                MDB.__maintainer_proc = Process(
                    target=maintainer,
                    daemon=False,
                    kwargs={
                        # Передаем все что нужно (для всех способов создания дочернего процесса)
                        'db_path': self.path, 'shm_name': self.salt,
                    }
                )
                
                MDB.__maintainer_proc.start()

                # Должны не выходя из блокировки дождаться подключения к SharedMemory

                while not self.shm:
                    sleep(MDB.TICK)
                    try:
                        self.shm = SharedMemory(name=self.salt, create=False)
                        logging.info(f"Attach SharedMemory {self.shm.name}, process {current_process().name}")
                    except FileNotFoundError:
                        pass

            # Если мы дошли сюда, то нужно присоединится к свободному слоту данных
            # Если слоты исчерпаны то ждем освобождения вечно и под системной блокировкой (другие ждут выше на SysLock)
            # После присоединения выходим из блокировки давая дорогу другим ожидающим
            captured = False
            while not captured:
                for self.index, locked in enumerate(MDB.__get_locks(self.shm.buf)):
                    if not locked:  # Есть свободный слот
                        self.shm.buf[MDB.__seek_lock(self.index)] = captured = True
                        break
                else:
                    # Нужно ждать лучших времен
                    sleep(MDB.TICK)

            self.index = self.index % MDB.MAX_PROCESSES;  # Индекс слота для __seek()
            logging.info(f"Capture DB Slot {self.index}, process {current_process().name}")
            

        

    def close(self):
        # Нужно для освобождения слота (обнуления захвата слота).
        if self.shm:
            if self.index >= 0:
                # Закрытие итераторов и только потом обнуление блокировок (в maintainer-е)
                self.shm.buf[MDB.__seek_lock(self.index)] = MDB.LOCK_CLEAN

            try:
                self.shm.close()
            except:
                pass
            
            self.shm = None
            self.index = -1


    def __del__(self):
        self.close()
        
            
    @staticmethod
    def __maintainer(*, db_path, shm_name):  # pylint: disable=W0238,W0613
        """
            Задача быстро без ожиданий обрабатывать заявки обращения к DB
            (из одного процесса levelDB поддерживает рандомный доступ)
        """
        import signal

        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

        # Создание нового SharedMemory. FileExistsError не может быть (возможность проверяется под системной блокировкой ОС)
        
        shm = SharedMemory(
            name=shm_name, create=True,
            size= MDB.MAX_PROCESSES + MDB.MAX_PROCESSES + MDB.MAX_PROCESSES * MDB.BLOCK_SIZE
        )
            
        logging.info(f"Create SharedMemory {shm.name}, process {current_process().name}")


        def _put_responce(index, data: dict):
            MDB.__put_data(shm.buf, index, data)

            shm.buf[MDB.__seek_state(index)] = MDB.STATE_RESPONCE

        def _get_request(index):
            data = MDB.__get_data(shm.buf, index)

            return data

        def _batch(db):
            try:
                with db.write_batch() as wb:
                    try:
                        while True:
                            r = yield
                            if r is None: continue;  # XXX При старте генератора нас обязывают вначале послать send(None)
                            
                            method, key, val = r
                            
                            if method == 'put':
                                wb.put(key, val)
                                
                            elif method == 'delete':
                                wb.delete(key)
                            else:
                                raise RuntimeError(f"Unsupported batch method: {repr(method)[0:125]}...")

                    except GeneratorExit:  # Мы закрываем генератор во вне, поэтому нужно предотвратить очистку транзакций
                        pass
                            
            # except EOFError:  # Если что-то не по плану, и нужно в середине закрыть генератор и отменить все write_batch транзакции
            #     raise;        # XXX .throw(EOFError) все равно бросит исключение во вне и без re-raise оно не будет EOFError

            finally:
                pass


        iterators = [None] * MDB.MAX_PROCESSES;  # Один итератор на процесс (желателен it.close() для итераторов созданных вне for)
        batches = [None] * MDB.MAX_PROCESSES;

        
        try:

            db = DB(db_path)
            
            while any(MDB.__get_locks(shm.buf)):
            
                # Ищем не idle - задачи
                tasks = MDB.__get_tasks(shm.buf)

                # Нас интересуют только запросы
                for i, t in enumerate(tasks):

                    if t == MDB.STATE_REQUEST:

                        # в i - номер задачи (index)
                        try:
                            request = _get_request(i)
                            if not isinstance(request, dict):
                                raise RuntimeError(f"Malformed request data: {repr(request)[0:125]}...")

                            method = request.get('method')

                            if method == 'put':
                                key, val = request.get('key'), request.get('val')

                                db.put(key, val)
                                
                                _put_responce(i, {'result': True})
                                                                
                            elif method == 'delete':
                                key = request.get('key')
                                
                                db.delete(key)
                                
                                _put_responce(i, {'result': True})
                                
                            elif method == 'get':
                                key = request.get('key')

                                val = db.get(key)

                                _put_responce(i, {'result': val})



                            elif method == 'iterator':
                                prefix, reverse, seek = request.get('prefix'), request.get('reverse'), request.get('seek')

                                if iterators[i] is not None:
                                    raise RuntimeError("Nesting iterators")
                                
                                iterators[i] = db.iterator(prefix, reverse, seek=seek)

                                _put_responce(i, {'result': True})

                            elif method == 'next':                             # при reverse=True возвращает следующее после seek
                                try:
                                    key_val = next(iterators[i]);  # key_val кортеж из ключа и значения

                                    _put_responce(i, {'result': key_val})
                                    
                                except StopIteration:
                                    iterators[i].close(); iterators[i] = None 

                                    _put_responce(i, {'result': 'StopIteration'})
                                    
                            elif method == 'close':  # close как и в обычных итераторах может быть многократным
                                if iterators[i]:
                                    iterators[i].close(); iterators[i] = None 

                                _put_responce(i, {'result': True})


                            elif method == 'batch_enter':
                                
                                if batches[i] is not None:
                                    raise RuntimeError("Nesting batches")
                                
                                batches[i] = _batch(db); batches[i].send(None);  # Запуск генератора

                                _put_responce(i, {'result': True})

                            elif method == 'batch_put':
                                key, val = request.get('key'), request.get('val')

                                batches[i].send( ('put', key, val) )
            
                                _put_responce(i, {'result': True})

                            elif method == 'batch_delete':
                                key = request.get('key')
                                
                                batches[i].send( ('delete', key, None) )
                                
                                _put_responce(i, {'result': True})                          

                            elif method == 'batch_exit':
                                batches[i].close(); batches[i] = None

                                _put_responce(i, {'result': True})

                            elif method == 'batch_error':
                                try:
                                    batches[i].throw(EOFError)
                                except EOFError: pass
                                finally:
                                    batches[i].close(); batches[i] = None

                                _put_responce(i, {'result': True})


                            else:
                                raise RuntimeError(f"Unsupported method: {repr(method)[0:125]}...")

                                
                        except Exception as e:
                            _put_responce(i, {'error': str(e)});  # XXX assert BLOCK_SIZE >= 512 (Ошибка должна влезть всегда)


                    # Клининг итераторов - Это происходит когда объект подключения так или иначе закрывается
                    # Если будет новое подключение то он либо займет новый слот или этот-же
                    # (так как мы даем shm.buf[MDB.__seek_lock(i)] = False)
                    
                    lock = shm.buf[MDB.__seek_lock(i)]
                    if lock == MDB.LOCK_CLEAN:
                        if iterators[i]:
                            iterators[i].close(); iterators[i] = None
                        if batches[i]:
                            try:
                                batches[i].throw(EOFError)
                            except EOFError: pass
                            finally:
                                batches[i].close(); batches[i] = None

                        shm.buf[MDB.__seek_lock(i)] = False;  # Слот освобожден для новых захватов. 1 байт - атомарная операция


                sleep(MDB.TICK)
                    
        finally:

            # По идеи здесь any(__get_locks[shm.buf]) == False
            # Но на всякий непредвиденный случай (исключений), чтобы ничего в памяти не висело
            
            for i, it in enumerate(iterators):
                if it: it.close(); iterators[i] = None
                    
            for i, bt in enumerate(batches):
                if bt:
                    try:
                        bt.throw(EOFError)
                    except EOFError: pass
                    finally:
                        bt.close(); batches[i] = None

            MDB.__clr_locks(shm.buf)

            try:
                shm.close()
                shm.unlink()
            except:
                pass
            
            logging.info(f"Clean SharedMemory {shm.name}, maintainer {current_process().name}")

            # db.close()

    
    def _put_request(self, data: dict):
        MDB.__put_data(self.shm.buf, self.index, data)
        
        self.shm.buf[MDB.__seek_state(self.index)] = MDB.STATE_REQUEST
        
    def _get_responce(self):
        data = MDB.__get_data(self.shm.buf, self.index)

        return data

    
    def _wait_responce(self, idle=True):
        _lock, _state = MDB.__seek_lock(self.index), MDB.__seek_state(self.index)
        
        while self.shm.buf[_lock]:
            if self.shm.buf[_state] != MDB.STATE_RESPONCE:
                sleep(MDB.TICK)
            else:
                break
        else:
            # self.shm.buf[seek] = MDB.STATE_IDLE
            raise ConnectionError("Workflow completed")

            
        res = self._get_responce()

        if idle:
            self.shm.buf[_state] = MDB.STATE_IDLE;  # Освобождение слота - результат уже считали


        if 'error' in res:
            raise RuntimeError(res.get('error'))

        if 'result' not in res:
            raise RuntimeError("Unexpected Result")
        
        return res.get('result')
        
    ################################################## Публичный Интерфейс ##########################################
    
    def put(self, key, val):
        with self.plock:
            _lock = MDB.__seek_lock(self.index)
            
            while self.shm.buf[_lock]:
                self._put_request({'method': 'put', 'key': key, 'val': val})
                self._wait_responce();  # Ждем исполнения

                return

            raise ConnectionError("Workflow completed")
        

    def delete(self, key):
        with self.plock:
            _lock = MDB.__seek_lock(self.index)
            
            while self.shm.buf[_lock]:
                self._put_request({'method': 'delete', 'key': key})
                self._wait_responce();  # Ждем исполнения

                return
                
            raise ConnectionError("Workflow completed")


    def get(self, key):
        with self.plock:
            _lock = MDB.__seek_lock(self.index)
            
            while self.shm.buf[_lock]:
                self._put_request({'method': 'get', 'key': key})
                result = self._wait_responce();  # Ждем исполнения

                return result

            raise ConnectionError("Workflow completed")


    def iterator(self, prefix=None, reverse=False, *, seek=None):
        with self.plock:
            prefix = prefix or ''
            
            _lock = MDB.__seek_lock(self.index)
            
            while self.shm.buf[_lock]:

                self._put_request({'method': 'iterator', 'prefix': prefix, 'reverse': reverse, 'seek': seek})
                self._wait_responce(idle=False);     # Ждем исполнения без перехода в idle (без освобождения слота)

                try:
                    while self.shm.buf[_lock]:
                        self._put_request({'method': 'next'})
                        
                        result = self._wait_responce(idle=False)
                        
                        if result == 'StopIteration':
                            return
                            
                        yield result;  # key, val

                    raise ConnectionError("Workflow completed")

                finally:  # return / iterator().close() Заставит поток кода попасть сюда (GeneratorExit)
                    if self.shm.buf[_lock]:
                        self._put_request({'method': 'close'})
                        self._wait_responce(idle=True)
                        
                    # return
                
            raise ConnectionError("Workflow completed")


    def write_batch(self):
        """
            Последовательная запись с откатом если в середине исключение.

            XXX Важно! Под контекстом `with db.write_batch() as wb:` все обращения только через wb,
                       где предусмотрено оставление слота открытым до завершения операций !
        """           
        
        class WriteBatch:
            def __init__(self, owner):
                self.owner = owner

            def put(self, key, val):
                _lock = MDB._MDB__seek_lock(self.owner.index)
                
                while self.owner.shm.buf[_lock]:
                    self.owner._put_request({'method': 'batch_put', 'key': key, 'val': val})
                    self.owner._wait_responce(idle=False);  # Ждем исполнения без перехода в idle (без освобождения слота)

                    return
                    
                raise ConnectionError("Workflow completed")


            def delete(self, key):
                _lock = MDB._MDB__seek_lock(self.owner.index)
                
                while self.owner.shm.buf[_lock]:
                    self.owner._put_request({'method': 'batch_delete', 'key': key})
                    self.owner._wait_responce(idle=False);  # Ждем исполнения без перехода в idle (без освобождения слота)

                    return
                    
                raise ConnectionError("Workflow completed")

            def __enter__(self):                            # Менеджер контекста для последовательных синхронизированных операций put/get с оператором with
                try:
                    self.owner.plock.acquire()
                
                    _lock = MDB._MDB__seek_lock(self.owner.index)
                    
                    while self.owner.shm.buf[_lock]:
                        self.owner._put_request({'method': 'batch_enter'})
                        self.owner._wait_responce(idle=False)

                        return self
                        
                    raise ConnectionError("Workflow completed")
                    
                finally:
                    pass
            

            def __exit__(self, exc_type, exc_value, traceback):  # Возвращает None - само исключение обрабатывается во вне
                try:
                    _lock = MDB._MDB__seek_lock(self.owner.index)
                    
                    while self.owner.shm.buf[_lock]:
                        # XXX Может быть GeneratorExit но здесь это досрочный выход из внешнего генератора (если он используется) с отменой всех транзакций
                        if not exc_type:
                            self.owner._put_request({'method': 'batch_exit'})
                        else:
                            self.owner._put_request({'method': 'batch_error'})
                            
                        self.owner._wait_responce(idle=True);  # С освобождением слота

                        return
                        
                    raise ConnectionError("Workflow completed")
                    
                finally:
                    self.owner.plock.release()


        return WriteBatch(self);  # self is owner

