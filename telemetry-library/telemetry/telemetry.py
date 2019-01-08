
#
# Copyright 2019 Games Creators Club
#
# MIT License
#
from telemetry_common.telemetry_common import *


class Telemetry():

    def __init__(self):
        self.telemetry_id = "Here"
        self.streams = []

    def newStream(self, name):
        stream = TelemetryStreamDefinition(name)
        stream.buildCallback = self._buildStream
        return stream

    def _buildStream(self, stream):
        self.streams.append(stream)
        # do something with server so stream is


if __name__ == "__main__":

    t = Telemetry()

    s = t.newStream("new stream")
    s.addByte("x", signed=True)
    s.addWord("y", signed=True)
    s.addInt("z", signed=True)
    s.addLong("w", signed=True)
    s.build()

    print("Pack string is " + s.pack_string)

    s.log(1, 2, 3, 4)

    print("Next record is " + str(s.nextRecord()))