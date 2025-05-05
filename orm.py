#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""
    Общая межпроцессная база данных levelDB с ORM-интерфейсом

    Main Process только создает рабочий процесс обслуживания базы (slot 0), остальные процессы -
    только подключаются сингл-тонами к рабочему процессу (каждый к своему слоту). Детали в mdb.py

    Структуру таблиц планируем с индексом для быстрого доступа и порядком хранения данных:

        Упорядоченное хранилище данных:
            'table.0000000000': {};  # Ключи (id) в лексикографическом порядке (в порядке добавления)
            'table.0000000001': {};  # Префикс таблицы данных ('table.') должен быть отличим от
            'table.    ...   ': {};  # префикса индекса ('tables.'): 'tables.' = 'table.'[:-1] + 's.'

        Поисковый индекс данных:
            'tables.<ckey>.<xxxxxxxxxx>': ...  # Все id соответствующие индексу ckey кодируются в префиксе в момент вставки новых данных
            'tables.<ckey>.<xxxxxxxxxx>': ...  # ckey - поисковый префикс в пределах которого все id удовлетворяют некоторому предикату
            'tables.<ckey>.<    ...   >': ...  # отображенному на ckey. Данные индекса (служебные) - не None значение (код lambda-предиката)

        Для служебных данных таблиц зарезервирован разделитель '#':
            'table#wcount': ...
        
    XXX eval вычисляет lambda в контексте этого модуля, поэтому если lambda зависима от классов импорта, то либо этот импорт должен быть
        явно здесь, либо во вне нужные классы могут быть добавлены так:

            from plyvelmp import orm; orm.User = User


    XXX Интерфейс через MDBModel использует наследников от MDBModel. Типичный пример использования:
    
        class User(MDBModel):
            # Все аннотированные поля - обязательны
            # Юзер с одним и тем-же uid может присутвовать в базе под разными ролями
            # (select() по умолчанию выбирает пересечение по ckey)

            uid: int | str;                       # Social-id (telegram-id, email-id) или любой уникальный хеш юзера вычислимый от данных юзера

            role: "agent | operator | admin" = 'user'

            info: dict = {};                      # Все default-значения перекидываются через copy(value)
                    
            ikeys = [
                lambda m: f"uid={m.uid}",         # В базе будет индекс 'Users.uid=<uid>.<tableID>': 'lambda m: ...'
                lambda m: f"role={m.role}",       # В базе будет индекс 'Users.role=<role>.<tableID>': 'lambda m: ...'
            ]

            def __eq__(self, other): return self.uid == other.get('uid') and self.role == other.get('role');  # == справа может быть dict
            def __ne__(self, other): return not self.__eq__(other);                                           # != справа может быть dict
                
            
            def __str__(self):
                info = self.info

                username = info.get('username')
                if username: return username

                username = ((info.get('first_name') or '') + ' ' + (info.get('last_name') or '')).strip()

                return username or self.uid



    32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63
       !  "  #  $  %  &  '  (  )  *  +  ,  -  .  /  0  1  2  3  4  5  6  7  8  9  :  ;  <  =  >  ?
"""


# pylint: disable=C0123,W0123,R0204

import sys, inspect, logging, re

from time import time
from copy import copy

from .lexoint import LexoInt
from .mdb import MDB


def memoized(func):
    memory = {}
    
    def wrap(*args, **kwargs):
        key = hash(repr(( *args, *sorted(kwargs.items()) )))
        if key not in memory:
            memory[key] = func(*args, **kwargs)
        return memory[key]

    wrap.__name__ = func.__name__
    return wrap



LEXOINT_SIZE = 16
TableID = "LexoInt | int | str"


class MDBModel(dict):
    """
        База для наследования моделей. Добавляет к словарю доступ к полям через точку
        а также это место декларирования поискового индекса в ikeys

        ikeys не попадают в словарь данных и явно в базу как поле данных не записываются
        ikey_default = 'items' добавляется всегда ко всем моделям (для возможности полной выборки c ckeys='items')

        lambd-ы в ikeys генерящие исключения индексируются в префиксе '...' (Ellipsis),
        возвращающие None - в префиксе 'None'
                
        default-значения полей могут быть вычислимыми через lambda (function.__name__ == '<lambda>')
        default-значения применяются только если не заданы фактические

        Аннотация полей - может быть произвольной кастомной meta-информацией для произвольного использования
        в наследниках ( извлечение через cls.annotations() )
        
        Поля без default-значения обязательны для заполнения (они аннотированы как минимум).
        Не аннотированные поля без default-значений не возможно задекларировать.
        

        XXX __str__ - может быть переопределена, но НЕ __repr__ ! (__repr__ используется для сериализации данных)

        Аннотации :TableID и дочерние классы от :MDBModel обрабатываются с преобразованием данных при доступе к
        полю как к атрибуту инстанца класса (например через точку) или при создании объекта




        FIXME Подумать о парадигме "Loop Unrolling" для оптимизации конструктора
        
    """
    
    id: TableID = None;  # Могут быть установлены значения по умолчанию (XXX Чистая Аннотация типа без значения НЕ создает атрибут класса)


    timestamp = lambda _self: time()

    
    ikeys = [            # Исходники генерации поискового индекса
        'items'
    ]


    @classmethod
    @memoized
    def annotations(cls):
        """
            Заглядывает в __annotations__ базовых классов
        """
        a = {}
        
        for c in cls.__mro__:
            if not issubclass(c, MDBModel): continue

            if hasattr(c, '__annotations__'):
                for field, ann in c.__annotations__.items():
                    # if field == 'id': continue
                    a.setdefault(field, ann);  # В наследниках аннотации могут перегружаться
                    
        return a

    @classmethod
    @memoized
    def defaults(cls):
        """
            Значения по умолчанию в декларация иерархии классов
        """
        d = {}

        for c in cls.__mro__:
            if not issubclass(c, MDBModel): continue

            for attr, value in c.__dict__.items():
                if attr == 'ikeys': continue
                if attr.startswith('__'): continue;                                              # Игнор системных полей
                
                if getattr(value, '__name__', None) == '<lambda>':                               # Вычислимое default-значение
                    d.setdefault(attr, value)
                    continue

                if callable(value) or isinstance(value, (staticmethod, classmethod)): continue;  # Пропуск пользовательских утилит в классе

                d.setdefault(attr, value);                                                       # Обычное поле
                
        return d


    @classmethod
    @memoized
    def comparables(cls):
        return (set(cls.annotations().keys()) | set(cls.defaults().keys())) - {'id', 'timestamp'}

            

    def __set_defaults(self):
        #  Не может затереть уже присутствующее значение в словаре
        for attr, value in self.defaults().items():
            
            if attr not in self:
                if callable(value):
                    setattr(self, attr, copy(value(self)))
                else:
                    setattr(self, attr, copy(value))

                        
    def __check_required(self):
        # Все что аннотировано обязано быть в словаре (хотя бы в виде default-значений)
        for field in self.annotations():
            if field not in self:
                e = TypeError(f"Required field: '{field}'"); e.field = field
                raise e

    @staticmethod
    def __cast_value(value, ann):
        if value is not None:
            if ann is TableID:
                return LexoInt(value, size=LEXOINT_SIZE);              # Приведение id к LexoInt
            elif isinstance(ann, type) and issubclass(ann, MDBModel):  # Словари преобразуем в объекты MDBModel
                if type(value) is not ann:
                    return ann(value);                                 # Вызов конструктора MDBModel
        return value
        
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if 'ikeys' in self:  # Это всегда только в классе (всегда default-значение)
            # cls.ikeys = self['ikeys']; del self['ikeys']
            raise AssertionError('ikeys can Not Change after Declaration')

        self.__set_defaults(); self.__check_required();  

        for field, ann in self.annotations().items():  # После __check_required() аннотированые self[field] точно есть
            self[field] = self.__cast_value(self[field], ann)

            
    def __setattr__(self, key, value):
        """
            id нужно только при обновлении данных. при вставке id затирается фактическим.
            ikeys нельзя менять динамически
        """
        if key == 'ikeys':
            raise AssertionError('ikeys can Not Change after Declaration')

        if key in (a := self.annotations()):
            value = self.__cast_value(value, a[key])
            
        self[key] = value


    def __getattribute__(self, item):
        if item in self:
            
            value = self[item]
            if item in (a := self.annotations()):
                value = self.__cast_value(value, a[item])
                
            return value
            
        else:
            return super().__getattribute__(item);  # Смотрит в уровни классов (в конце вернет default значения, если есть)


    def __delattr__(self, item):
        if item == 'ikeys':
            raise AssertionError('ikeys can Not Change after Declaration')

        val = self.pop(item)
        
        try:
            self.__set_defaults(); self.__check_required()
            
        except Exception:
            self.setdefault(item, val);  # Восстанавливаем как было до исключения
            raise


    def __str__(self):
        """
            Форматер представления данных
        """
        return super().__str__()


    def __eq__(self, other):
        """
            other может быть dict
        """
        return { k: self.get(k) for k in self.comparables() } == { k: other.get(k) for k in self.comparables() }
        
    def __ne__(self, other):
        return not self.__eq__(other)
        
    def __hash__(self):
        """
            dict сам по себе не хешируемый тип
            XXX При создании объектов timestamp делает объекты разными, но copy - одинаковыми (так как словари получается одинаковым)
        """
        
        return hash(repr({ k: self.get(k) for k in self.comparables() }))




class MDBOrm(MDB):
    """
        Интерфейс MDB с дополнительными методами, заточенными под наследников MDBModel:
            insert, update, select, remove
        
        XXX Низкоуровневый интерфейс в базе MDB (put, get, delete, iterator, write_batch) потоко-безопасный: self.plock )

        XXX Помним что представление совокупных данных (ключ, значение, служебка) ограничено MDB.BLOCK_SIZE
    """
    
    ikeys_cache = {};   # Для кеширования результатов с inspect.getsource()

    select_caches = {};  # select() кешируется потаблично до первой операции записи в таблицу

    @staticmethod
    def __calculate_index(ikeys_set, data):  # -> (ckeys, ikeys)
        ckeys = []; ikeys = []
        for ikey in ikeys_set:
            try:
                ckey = eval(ikey)
            except BaseException:
                ckey = ikey
            try:
                if callable(ckey): ckey = ckey(data);  # Предикат индекса на data
            except BaseException as e:
                logging.exception(e)
                ckey = ...;  # Ellipsis
                
            ckeys.append(str(ckey)); ikeys.append(str(ikey));  # XXX str() от строки есть та-же строка       
        return ckeys, ikeys


    def __init__(self, path='DB'):

        cls = type(self)
        if (SALT := cls.__dict__.get('SALT')) is not None:
            super_cls = cls.__mro__[1:2]; super_cls = super_cls and super_cls[0]
            if super_cls:
                super_cls.SALT = super_cls.__qualname__ + SALT
            
        
        super().__init__(path)


    def _insert(self, table, /, data: dict, *, ikeys='items'):
        """
            Вставка новых данных в базу
            
            ikeys - список функций получения поисковых префиксов из данных (по умолчанию - все данные в 'items')
                    XXX тела lambda записываются в индекс как способ вычисления
                        None-результаты lambda (как и исключения) НЕ игнорируются индексом, чтобы были тела lambda для последующих обновлений

            FIXME - eval не очень безопасно (следить за тем чтобы lambd-ы возвращали только строки)
                    ikey если не лямбда, то не вычислимая через eval() хрень
        """
        

        if not isinstance(data, dict): raise TypeError(f"Incompatible data type: {type(data)}")

        ikeys = ikeys if ikeys is not None else []
        
        if not table.endswith('.'): table += '.'
        if not isinstance(ikeys, (list, tuple, set, frozenset)): ikeys = [ikeys]

        # Создаем множество поискового индекса, пригодное для записи в базу
        ikeys_set = set(); ikeys_set.add('items')
        for ikey in ikeys:
            if not callable(ikey):
                ikeys_set.add(str(ikey))
                
            elif ikey in MDBOrm.ikeys_cache:
                ikeys_set.add(MDBOrm.ikeys_cache[ikey])
                
            else:
                lambda_src = inspect.getsource(ikey) or ''
                lambda_src = re.sub(r'#.*$', '', lambda_src, 1)
                lambda_src = lambda_src.strip("\r\n\t\f ,;")

                # Проверка, что это lambda-выражение
                try:
                    assert eval(lambda_src).__name__ == "<lambda>"

                    ikeys_set.add(lambda_src)
                    MDBOrm.ikeys_cache[ikey] = lambda_src
                    
                    logging.debug(f"Add lambda ikey '{lambda_src}' to cache")
                    

                except BaseException as e:
                    # logging.exception(e)
                    raise AssertionError(f"Index key {lambda_src} IS not pure lambda expression: {e}") from None                        
                    

        # Теперь ikeys_set это либо строка, либо строка lambda-функции вычислимой в контексте этого модуля через eval

        with self.plock:
            lexocount = self._lexocount(table);                 # Это следующий id записи

            wcount = super().get(table[:-1] + '#wcount') or 0;  # Счетчик записей


            data.update(id=lexocount);           # Обновит или установит id в data по ссылке

            if 'ckeys' in data: del data['ckeys']

            ckeys, ikeys = self.__calculate_index(ikeys_set, data)

            data['ckeys'] = ckeys;  # Храним и служебные данные (чтобы знать что удалять потом в индексе)

            
            
            with super().write_batch() as wb:    # Как единая неразрывная транзакция
                # Все обращения только через wb, где предусмотрено оставление слота открытым до завершения операций
                for ckey, ikey in zip(ckeys, ikeys):
                    wb.put(table[:-1] + 's.' + ckey + '.' + lexocount, ikey)
                wb.put(table + lexocount, data)

                wb.put(table[:-1] + '#wcount', wcount + 1)
                

            self.select_caches.pop(table, None)


    def insert(self, data: 'MDBModel', /):
        self._insert(type(data).__name__, data, ikeys=data.ikeys)


    def _remove(self, table, /, data: dict):
        """
            Фактически в data для удаления нужен только id, и он должен быть подан {'id': 'xxxxxxxxxx'}
            
            Удаляем фактически (остаются дыры в упорядоченном хранилище)
            При удалении если чего-то нет и целостность не нарушена, то исключения не нужны
            
            XXX одно и тоже можно удалять много раз
        """
        if not isinstance(data, dict): raise TypeError(f"Incompatible data type: {type(data)}")
        
        if not table.endswith('.'): table += '.'

        with self.plock:
            # Удаляем по данным
            if data is not None:
                try:
                    lexocount = str(LexoInt(data.get('id'), size=LEXOINT_SIZE))
                except Exception:
                    return

                wcount = super().get(table[:-1] + '#wcount') or 0
                    
                data = super().get(table + lexocount)
                if data is not None:  # Еще не удалялось
                    ckeys = data['ckeys']
                    with super().write_batch() as wb:
                        for ckey in ckeys:
                            try:
                                wb.delete(table[:-1] + 's.' + str(ckey) + '.' + lexocount)
                            except Exception: pass
                        wb.delete(table + lexocount)

                        wb.put(table[:-1] + '#wcount', wcount + 1)

                    self.select_caches.pop(table, None)
                        

    def remove(self, data: "MDBModel", /):
        self._remove(type(data).__name__, data)

    
    def _update(self, table, /, data: dict):
        """
           Фактически мы должны при обновлении данных удалить старые и записать новые
           с новым поисковым индексом но под тем-же id (lexocount).
           
           lambda для вычисления ключей индекса (ckey) - в самом индексе в виде строк или
           вместо lambda готовый ключ в виде строки
        """
        if not isinstance(data, dict): raise TypeError(f"Incompatible data type: {type(data)}")
        
        if not table.endswith('.'): table += '.'
        if 'ckeys' in data: del data['ckeys']

        with self.plock:                

            try:
                lexocount = str(LexoInt(data.get('id'), size=LEXOINT_SIZE))
            except Exception:
                raise ReferenceError(f"Update with invalid id '{data.get('id')}'") from None
                

            wcount = super().get(table[:-1] + '#wcount') or 0

            
            _data = super().get(table + lexocount)
            if _data is None:
                raise LookupError(f"Updating non-existing data with id '{lexocount}'")
            
            _ckeys = _data['ckeys'];  # Это старые ключи индекса tables.<ckey>.<lexocount>: lambda data: ...

            _get = super().get; ikeys_set = { _get(table[:-1] + 's.' + ckey + '.' + lexocount) for ckey in _ckeys };  # Как вычислялись

            # Формируем обновленные данные с тем-же id но без поисковых ключей
            
            _data.update(data); data.update(_data);  # Полный синхрон

            # Формируем новые поисковые ключи

            ckeys, ikeys = self.__calculate_index(ikeys_set, data)

            data['ckeys'] = ckeys;  # Новые поисковые ключи

            with super().write_batch() as wb:    # Все обновление как неразрывная транзакция
                # Вначале удаляем старое
                for ckey in _ckeys:  
                    try:
                        wb.delete(table[:-1] + 's.' + str(ckey) + '.' + lexocount)
                    except Exception: pass
                          
                # Записываем новые данные
                for ckey, ikey in zip(ckeys, ikeys):
                    wb.put(table[:-1] + 's.' + ckey + '.' + lexocount, ikey)

                # Последний шрих
                wb.put(table + lexocount, data)

                wb.put(table[:-1] + '#wcount', wcount + 1)

            self.select_caches.pop(table, None)
                

    def update(self, data: "MDBModel", /):
        self._update(type(data).__name__, data)

    
    def _select(self, table, /,
                reverse=True, mode: "union | inter" = 'inter', *,
                
                ckeys='items',

                seek = None, limit=sys.maxsize,
                
                ) -> list:
        """
            Выборка по поисковому индексу. ckeys - это уже вычисленные ключи индекса

            XXX По разным ckeys делается пересечение (по умолчанию) или объединение, а дубликаты из выборки исключаются

            seek - смещение на заданный id перед выборкой (может точно не совпадать). Драйвер базы откатывается на
                   seek в лексикографическом порядке и останавливается когда дальнейший поиск смещения не нужен (дальше
                   уже лексикографически большие/меньшие ключи). С этой точки и будет первая итерация next в сторону
                   заданную reverse. При этом если seek точно совпал с ключем, то для reverse=False этот ключь входит в выборку,
                   (next начинается с него), а для reverse=True - пропускается (next начинается с предшествующего).
                   Если seek не совпал и стал как были посередине ключей, то next захватит все следующие (reverse=False)
                   или все предшествующие (reverse=True) ключи

            XXX Алгоритм пагинации (c limit):
               - при reverse=False изначально seek=None, затем равен последнему id в выборке, увеличенному на 1-цу;
               - при reverse=True изначально seek=None, затем равен последнему id в выборке, уменьшенному на 1-цу;
               - при limit=1 в селекции будет id, равный seek (если такой id есть), или следующий в направлении reverse
        """
        ckeys = ckeys if ckeys is not None else ['']
        
        if not table.endswith('.'): table += '.'
        if not isinstance(ckeys, (list, tuple, set, frozenset)): ckeys = [ckeys]


        ids = set(); result = [];

        intersection = mode.startswith('inter')


        count = 0

        if seek is not None:
            seek = LexoInt(seek, size=LEXOINT_SIZE)
            # if not reverse: seek += 1
            if reverse: seek += 1
            seek = str(seek)
            

        if limit > 0:
            with self.plock:

                key_cache = hash(repr( (reverse, intersection, ckeys, seek, limit) ))
                cache = MDBOrm.select_caches.setdefault(table, {});  # cache ссылка на {<key_cache>: result, ...}
                if key_cache in cache:
                    return cache[key_cache]
                
                
                for ckey in ckeys:
                    
                    for lexocount, _ in super().iterator(table[:-1] + 's.' + str(ckey) + '.', reverse=reverse, seek=seek):
                        if lexocount not in ids:
                            ids.add(lexocount)
                            
                            if self._islexointstring(lexocount):
                                val = super().get(table + lexocount)
                                if not intersection:
                                    result.append(val)
                                    if (count := count + 1) >= limit: break
                                    
                                    
                                else:
                                    _ckeys = val.get('ckeys') or []
                                    if all( str(k) in _ckeys for k in ckeys ):
                                        result.append(val)
                                        if (count := count + 1) >= limit: break
                    else:
                        continue

                    break


                cache[key_cache] = result

                                    
        return result


    def select(self, Model: "type(MDBModel)", /,
               reverse=True, mode: "union | inter" = 'inter', *,
               
               ckeys='items',

               seek = None, limit=sys.maxsize,
               
               ) -> "List[MDBModel]":

        return [ Model(data) for data in self._select(Model.__name__, reverse, mode, ckeys=ckeys, seek=seek, limit=limit) ];  # Всегда создает копии Model(data)


    def _getrow(self, table, /, table_id):

        table_id = LexoInt(table_id, size=LEXOINT_SIZE)
        
        data = (r := self._select(table, reverse=True, seek = table_id + 1, limit=1)) and r[0] or None
        if not data: return None

        lexocount = LexoInt(data.get('id'), size=LEXOINT_SIZE)
        if lexocount != table_id: return None

        return data

    def getrow(self, Model: "type(MDBModel)", /, table_id):
        return Model(data) if (data := self._getrow(Model.__name__, table_id)) else None

        
        
    def _lexocount(self, table):  # table is prefix
        last_index = None
        
        with self.plock:
            for idx, _ in super().iterator(table, reverse=True):
                if not self._islexointstring(idx): continue;  # Просматриваем только LexoInt
                
                last_index = idx
                break

        return str(LexoInt(last_index, size=LEXOINT_SIZE) + 1) if last_index is not None else str(LexoInt('0' * LEXOINT_SIZE))


    @staticmethod
    def _islexointstring(lexocount):

        LEXOINT_REGEX = globals().setdefault('LEXOINT_REGEX', r'[0-9]{' + str(LEXOINT_SIZE) + r'}')
        
        return bool(lexocount and isinstance(lexocount, str) and re.fullmatch(LEXOINT_REGEX, lexocount))

    
    def _wcount(self, table):
        if not table.endswith('.'): table += '.'
        return super().get(table[:-1] + '#wcount') or 0

    def wcount(self, Model: "type(MDBModel)"):
        return self._wcount(Model.__name__)



        
