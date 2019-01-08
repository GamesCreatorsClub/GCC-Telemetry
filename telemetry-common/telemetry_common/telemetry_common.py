
#
# Copyright 2019 Games Creators Club
#
# MIT License
#

import struct

STREAM_ID_BYTE = 1
STREAM_ID_WORD = 2
STREAM_SIZE_BYTE = 4
STREAM_SIZE_WORD = 8
STREAM_SIZE_LONG = 16

TYPE_BYTE = 'b'
TYPE_WORD = 'w'
TYPE_INT = 'i'
TYPE_LONG = 'l'
TYPE_FLOAT = 'f'
TYPE_DOUBLE = 'd'
TYPE_STRING = 's'
TYPE_BYTES = 'a'


class TelemetryStreamField:
    def __init__(self, name, field_type, size):
        self.name = name
        self.field_type = field_type
        self._size = size

    def size(self, value):
        return self._size

    def _store(self, buffer, ptr, value):
        return ptr + self._size

    def _packFormat(self):
        return None


class TelemetryStreamByteField(TelemetryStreamField):
    def __init__(self, name, signed=False):
        super(TelemetryStreamByteField, self).__init__(name, TYPE_BYTE, 1)
        self.signed = signed

    def _store(self, buffer, ptr, value):
        if self.signed:
            buffer[ptr] = struct.pack('b', value)
        else:
            buffer[ptr] = struct.pack('B', value)
        return ptr + self._size

    def _packFormat(self):
        return 'b' if self.signed else 'B'


class TelemetryStreamWordField(TelemetryStreamField):
    def __init__(self, name, signed=False):
        super(TelemetryStreamWordField, self).__init__(name, TYPE_WORD, 2)
        self.signed = signed

    def _store(self, buffer, ptr, value):
        if self.signed:
            buffer[ptr:ptr+2] = struct.pack('h', value)
        else:
            buffer[ptr:ptr+2] = struct.pack('H', value)
        return ptr + self._size

    def _packFormat(self):
        return 'h' if self.signed else 'H'


class TelemetryStreamIntField(TelemetryStreamField):
    def __init__(self, name, signed=False):
        super(TelemetryStreamIntField, self).__init__(name, TYPE_INT, 4)
        self.signed = signed

    def _store(self, buffer, ptr, value):
        if self.signed:
            buffer[ptr:ptr+4] = struct.pack('i', value)
        else:
            buffer[ptr:ptr+4] = struct.pack('I', value)
        return ptr + self._size

    def _packFormat(self):
        return 'i' if self.signed else 'I'


class TelemetryStreamLongField(TelemetryStreamField):
    def __init__(self, name, signed=False):
        super(TelemetryStreamLongField, self).__init__(name, TYPE_LONG, 8)
        self.signed = signed

    def _store(self, buffer, ptr, value):
        if self.signed:
            buffer[ptr:ptr+8] = struct.pack('q', value)
        else:
            buffer[ptr:ptr+8] = struct.pack('Q', value)
        return ptr + self._size

    def _packFormat(self):
        return 'q' if self.signed else 'Q'


class TelemetryStreamFloatField(TelemetryStreamField):
    def __init__(self, name):
        super(TelemetryStreamFloatField, self).__init__(name, TYPE_FLOAT, 4)

    def _store(self, buffer, ptr, value):
        buffer[ptr:ptr+4] = struct.pack('f', value)
        return ptr + self._size

    def _packFormat(self):
        return 'f'


class TelemetryStreamDoubleField(TelemetryStreamField):
    def __init__(self, name):
        super(TelemetryStreamDoubleField, self).__init__(name, TYPE_DOUBLE, 8)

    def _store(self, buffer, ptr, value):
        buffer[ptr:ptr+8] = struct.pack('d', value)
        return ptr + self._size

    def _packFormat(self):
        return 'd'


class TelemetryStreamStringField(TelemetryStreamField):
    def __init__(self, name, size):
        super(TelemetryStreamStringField, self).__init__(name, TYPE_STRING, size)

    def _store(self, buffer, ptr, value):
        length = len(value)
        buffer[ptr:ptr+self._size] = struct.pack(str(self._size) + 'd', value)
        return ptr + self._size

    def _packFormat(self):
        return str(self._size) + 'p'


class TelemetryStreamBytesField(TelemetryStreamField):
    def __init__(self, name, size):
        super(TelemetryStreamBytesField, self).__init__(name, TYPE_BYTES, size)

    def _store(self, buffer, ptr, value):
        length = len(value)
        buffer[ptr:ptr+self._size] = value
        return ptr + self._size

    def _packFormat(self):
        return str(self._size) + 's'


class TelemetryStreamDefinition:

    def __init__(self, name):
        self.name = name
        self.id = 0  # Not defined yet
        self.buildCallback = None
        self.fields = []
        self.fixed_length = 0

    def addByte(self, name, signed=False):
        self.fields.append(TelemetryStreamByteField(name, signed))
        return self

    def addWord(self, name, signed=False):
        self.fields.append(TelemetryStreamWordField(name, signed))
        return self

    def addInt(self, name, signed=False):
        self.fields.append(TelemetryStreamIntField(name, signed))
        return self

    def addLong(self, name, signed=False):
        self.fields.append(TelemetryStreamLongField(name, signed))
        return self

    def addFloat(self, name):
        self.fields.append(TelemetryStreamFloatField(name))
        return self

    def addDouble(self, name):
        self.fields.append(TelemetryStreamDoubleField(name))
        return self

    def addFixedString(self, name, size):
        self.fields.append(TelemetryStreamStringField(name, size))
        return self

    def addFixedBytes(self, name, size):
        self.fields.append(TelemetryStreamBytesField(name, size))
        return self

    def addVarLenString(self, name, size):
        raise NotImplemented("Not implemented yet")

    def addVarLenBytes(self, name, size):
        raise NotImplemented("Not implemented yet")

    def getFields(self):
        return self.fields

    def build(self):
        self.fixed_length = 0
        self.pack_string = ""
        for field in self.fields:
            if self.fixed_length is not None:
                field_len = field._size
                if field_len > 0:
                    self.fixed_length += field_len
                    self.pack_string += field._packFormat()
                else:
                    self.fixed_length = None
                    self.pack_string = None

        if self.pack_string is not None:
            self.pack_string = '<' + self.pack_string
        self.buildCallback(self)

    def log(self, timestamp, *args):
        if self.fixed_length is None:
            raise NotImplemented("Variable record size len is not yet implemented")

        bytes = struct.pack(self.pack_string, *args)
        print("Logged bytes " + str(bytes))

    def nextRecord(self):
        return struct.unpack(self.pack_string, b'\x01\x02\x00\x03\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00')


if __name__ == "__main__":
    pass
