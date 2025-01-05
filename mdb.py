#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import logging, multiprocessing, os, warnings

from multiprocessing import shared_memory, resource_tracker

from time import sleep, time

from ast import literal_eval

from .db import DB


class ExhaustedError(RuntimeError): pass


# ISSUE UserWarning: resource_tracker: There appear to be 1 leaked shared_memory objects to clean up at shutdown
# resource_tracker.unregister(self.shm._name, 'shared_memory')
# warnings.filterwarnings('ignore')
# with warnings.catch_warnings(action="ignore"): ...
# warnings.simplefilter("ignore")


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
            [ lock0, lock1, ...,    uid0, uid1, ...,    state0, state1, ...,    data0, data1, ... ],
            где lock  - флаги захваченных процессами слотов;
                uid   - данные разрешения коллизий при захвате;
                state - байт состояния для синхронизации: 0 - idle, 1 - request, 2 - responce;
                data  - данные ввода/вывода в литеральных b-строках (образованных от словаря);
    """

    BLOCK_SIZE = DB.BLOCK_SIZE;  # Определяет максимальное число байт в data
    # SALT = DB.SALT
    SALT = __qualname__ + 'muqpjaTWTcwHmmqL';  # alphabet = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'

    assert BLOCK_SIZE >= 512

    # Мы можем подключаться из другой программы, где объекты синхронизации не доступны (работаем в режиме software lock)
    UID_SIZE = 32; TICK = 0.00001;

    assert UID_SIZE % 2 == 0; assert TICK > 0.0000001;


    MAX_PROCESSES = 24;  # Определяет размер SharedMemory для обслуживания процессов (максимальное число процессов)

    
    STATE_IDLE, STATE_REQUEST, STATE_RESPONCE = range(3);  # XXX b'\0'[0] == 0, b'\1'[0] == 1, ...

    LOCK_FREE, LOCK_LOCK, LOCK_CLEAN = range(3);           # False, True, 2

    @staticmethod
    def __seek_lock(index): return 0 + (index % MDB.MAX_PROCESSES)

    @staticmethod
    def __seek_uid(index): return 0 + MDB.MAX_PROCESSES + (index % MDB.MAX_PROCESSES) * MDB.UID_SIZE
    
    @staticmethod
    def __seek_state(index): return 0 + MDB.MAX_PROCESSES + MDB.MAX_PROCESSES * MDB.UID_SIZE + (index % MDB.MAX_PROCESSES)
            
    @staticmethod
    def __seek_data(index): return 0 + MDB.MAX_PROCESSES + MDB.MAX_PROCESSES * MDB.UID_SIZE + MDB.MAX_PROCESSES + (index % MDB.MAX_PROCESSES) * MDB.BLOCK_SIZE


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

    @staticmethod
    def __clr_uids(shm_buf):
        shm_buf[MDB.__seek_uid(0): MDB.__seek_uid(MDB.MAX_PROCESSES - 1) + MDB.UID_SIZE] = b'\0' * MDB.UID_SIZE * MDB.MAX_PROCESSES
            
        
    def __init__(self, path='DB'):
        
        self.path = path

        self.salt = MDB.SALT + path.replace(os.path.pathsep, '').replace(os.path.sep, '')

        self.__process = None
        
        self.__uid = (u := os.urandom(MDB.UID_SIZE // 2)) + u;   # Дубликат справа играет роль контрольной суммы

        self.shm = None; self.shm_creator = False; self.index = -1

        try:
            # Аттач к существующей SharedMemory
            self.shm = shared_memory.SharedMemory(name=self.salt, create=False)
            logging.info(f"Attach SharedMemory {self.shm.name}, process {multiprocessing.current_process().name}")
            
        except FileNotFoundError:
            # Создание нового SharedMemory
            if not multiprocessing.parent_process():
                try:
                    self.shm = shared_memory.SharedMemory(
                        name=self.salt, create=True,
                        size= 0 + MDB.MAX_PROCESSES + MDB.MAX_PROCESSES * MDB.UID_SIZE + MDB.MAX_PROCESSES + MDB.MAX_PROCESSES * MDB.BLOCK_SIZE
                    )
                    
                    self.shm_creator = True
                    logging.info(f"Create SharedMemory {self.shm.name}, process {multiprocessing.current_process().name}")

                except FileExistsError:  # Может быть в мультипроцессной среде (одновременный пуск нескольких приложений)
                    self.shm = shared_memory.SharedMemory(name=self.salt, create=False)
                    logging.info(f"Attach SharedMemory {self.shm.name}, process {multiprocessing.current_process().name}")
            else:
                # XXX Это необходимо чтобы предсказуемо запустить процесс обслуживания после первой блокировки первого слота
                raise RuntimeError("Only Main Process must create SharedMemory") from None

                    
        # Если мы дошли сюда, то нужно разрешая возможную коллизию, присоединится к свободному слоту данных
        # Пытаемся захватить подряд свободные слоты до тех пор пока либо не обнаружим захват другим процессом,
        # либо не захватим сами, либо не исчерпаются свободные слоты

        
        buf = self.shm.buf
        for idx in range(MDB.MAX_PROCESSES**2):
            # XXX Мы не можем исключить пропуск слота во время разрешения коллизии и нужно пытаться снова по кругу
            
            seek = self.__seek_uid(idx)
            
            captured = False
            
            while not captured:
                st = time(); dt = 0.0
                
                locked = buf[self.__seek_lock(idx)]
                if not locked:
                
                    uid = bytes(buf[seek: seek + MDB.UID_SIZE])
                    if (uid == b'\0' * MDB.UID_SIZE or uid[0: MDB.UID_SIZE // 2] != uid[MDB.UID_SIZE // 2: MDB.UID_SIZE]):
                        # Либо свободный слот либо коллизионная борьба за захват
                        
                        # for i in range(MDB.UID_SIZE): buf[seek + i] = (buf[seek + i] ^ uid[i]) ^ self.__uid[i];  # Обнуляем мусор и добавляем uid
                        buf[seek: seek + MDB.UID_SIZE] = self.__uid

                        dt = time() - st;  # Оценка времени захвата
                        
                        sleep(MDB.TICK + dt * (1 + os.urandom(1)[0] / 256 * MDB.MAX_PROCESSES));  # Случайная пауза для разрешения коллизии

                        continue;  # На перепроверку
                        
                    # Решаем захватили ли мы или другой процесс
                    
                    if uid == self.__uid:
                        # XXX Это гарантировано только для одного процесса и только в этой точке кода
                        
                        buf[self.__seek_lock(idx)] = captured = True

                        continue

                    # Нужно подтереть за собой мусор и перейти к следующему слоту
                    # for i in range(MDB.UID_SIZE): buf[seek + i] = (buf[seek + i] ^ uid[i])
                    buf[seek: seek + MDB.UID_SIZE] = b'\0' * MDB.UID_SIZE
                        
                # Переход к следующему idx
                break
                    
            else:
                break;  # Успешный захват
            

        else:
            # Исчерпаны слоты
            # raise RuntimeError("Ran out of data slots in SharedMemory")
            raise ExhaustedError("Ran out of data slots in SharedMemory")

    
        self.index = idx % MDB.MAX_PROCESSES;  # Индекс слота для __seek()

        logging.info(f"Capture DB Slot {idx}, process {multiprocessing.current_process().name}")


        if self.shm_creator:
            # Запуск обслуживания запросов.
            # Один слот ка минимум к этому моменту захвачен и обслуживающий процесс знает когда ему завершится
            self.__process = multiprocessing.Process(
                target=maintainer,
                daemon=False,
                kwargs={
                    # Передаем все что нужно (для всех способов создания дочернего процесса)
                    'db_path':      self.path, 'shm_name':     self.salt,
                    
                    'shm':          self.shm,
                }
            )
            
            self.__process.start()


    def close(self):
        # Нужно для освобождения слота (обнуления данных захвата слота).
        if self.shm:
            if self.index >= 0:
                self.shm.buf[self.__seek_lock(self.index)] = MDB.LOCK_CLEAN;  # Закрытие итераторов и только потом обнуление блокировок

            try:
                self.shm.close()
            except:
                pass
            
            self.shm = None
            self.index = -1


    def __del__(self):
        self.close()
        
            
    @staticmethod
    def __maintainer(*, db_path, shm_name, shm):  # pylint: disable=W0238,W0613
        """
            Задача быстро без ожиданий обрабатывать заявки обращения к DB
            (из одного процесса levelDB поддерживает рандомный доступ)
        """
        
        import signal
        

        # Здесь мы аттачимся к уже созданному владельцем SharedMemory (здесь владелиц не отличим от остальных)
        # shm = shared_memory.SharedMemory(shm_name, create=False)
        # logging.info(f"Open SharedMemory {shm.name}, maintainer {multiprocessing.current_process().name}")


        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)


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


                        seek_uid = MDB.__seek_uid(i)
                        shm.buf[seek_uid: seek_uid + MDB.UID_SIZE] = b'\0' * MDB.UID_SIZE
                        shm.buf[MDB.__seek_lock(i)] = False



                sleep(MDB.TICK)
                    
        finally:

            # По идеи здесь any(__get_locks[shm.buf]) == False
            # Но на всякий непредвиденный случай (исключений)
            
            for i, it in enumerate(iterators):
                if it: it.close(); iterators[i] = None
                    
            for i, bt in enumerate(batches):
                if bt:
                    try:
                        bt.throw(EOFError)
                    except EOFError: pass
                    finally:
                        bt.close(); batches[i] = None

            MDB.__clr_uids(shm.buf)
            MDB.__clr_locks(shm.buf)

            try:
                shm.unlink()
            except:
                pass
            
            logging.info(f"Clean SharedMemory {shm.name}, maintainer {multiprocessing.current_process().name}")

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
                try: sleep(MDB.TICK)
                except: pass
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
        _lock = MDB.__seek_lock(self.index)
        
        while self.shm.buf[_lock]:
            self._put_request({'method': 'put', 'key': key, 'val': val})
            self._wait_responce();  # Ждем исполнения

            return

        raise ConnectionError("Workflow completed")
        

    def delete(self, key):
        _lock = MDB.__seek_lock(self.index)
        
        while self.shm.buf[_lock]:
            self._put_request({'method': 'delete', 'key': key})
            self._wait_responce();  # Ждем исполнения

            return
            
        raise ConnectionError("Workflow completed")


    def get(self, key):
        _lock = MDB.__seek_lock(self.index)
        
        while self.shm.buf[_lock]:
            self._put_request({'method': 'get', 'key': key})
            result = self._wait_responce();  # Ждем исполнения

            return result

        raise ConnectionError("Workflow completed")


    def iterator(self, prefix=None, reverse=False, *, seek=None):
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
                
                _lock = MDB._MDB__seek_lock(self.owner.index)
                
                while self.owner.shm.buf[_lock]:
                    self.owner._put_request({'method': 'batch_enter'})
                    self.owner._wait_responce(idle=False)

                    return self
                    
                raise ConnectionError("Workflow completed")                
            

            def __exit__(self, exc_type, exc_value, traceback):  # Возвращает None - само исключение обрабатывается во вне
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

        return WriteBatch(self);  # self is owner
