#!/usr/bin/env python3
# -*- coding: utf-8 -*-

def __LexoInt_magics_override__(name, bases, attrs):
    """
        Нужно перегрузить все магические методы, которые возвращают non-mutable значение базового класса
        и вернуть обертку из наследника над этим значением

        metaclass=__LexoInt_magics_override__ - работает на этапе декларации наследника
    """
    base = bases[0]
    
    for mtd_name, mtd in base.__dict__.items():
        
        if mtd_name in {'__add__', '__radd__', '__sub__', '__rsub__', '__mul__', '__rmul__', '__mod__', '__rmod__', '__pow__', '__rpow__', '__neg__',
                        '__pos__', '__abs__', '__invert__', '__lshift__', '__rlshift__', '__rshift__', '__rrshift__', '__and__', '__rand__', '__xor__',
                        '__rxor__', '__or__', '__ror__', '__int__', '__floordiv__', '__rfloordiv__', '__trunc__', '__floor__', '__ceil__', '__round__'}:
                 
            # mtd=mtd - захват текущего значения в цикле для замыкания
            
            attrs[mtd_name] = lambda self, *args, mtd=mtd, **kwargs: type(self)( mtd(self, *args, **kwargs), size=getattr(self, 'size', None) )

    return type(name, bases, attrs)

class LexoInt(int,  metaclass=__LexoInt_magics_override__):
    """
        Лексигрофический порядок чисел (для ключей в базе данных с контролируемым порядком вставки и извлечения)
    """
    def __new__(cls, *args, size=None, **kwargs):
        return super().__new__(cls, *args, **kwargs)
    
    def __init__(self, *args, size=None, **_kwargs):
        super().__init__();  # Некоторые объекты стандартной библиотеки инициализируются в __new__

        if len(args) > 0:
            literal = args[0]
            if not size and isinstance(literal, str):
                size = len(literal)

        self.size = size or 16;  # Размер строки представления


        if super().denominator != 1:
            raise ValueError(f"invalid literal for LexoInt: '{super().__repr__()}'")

        if super().numerator < 0:
            raise ValueError(f"invalid literal for LexoInt: '{super().__repr__()}'")

        if len(super().numerator.__repr__()) > self.size:
            raise ValueError(f"invalid literal for LexoInt: '{super().__repr__()}'")
        

    def __str__(self):
        return super().__repr__().zfill(self.size)

    def __repr__(self):
        return "'" + self.__str__() + "'"


class LexoInt10(LexoInt):
    def __init__(self, *args, **kwargs):
        kwargs.pop('size', None)
        super().__init__(*args, size=10, **kwargs)

