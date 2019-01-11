from telemetry_server.telemetry_server import *
from telemetry.telemetry_logger import *
from telemetry.telemetry_client import *


subs = {}


def sub(topic, callback):
    if topic not in subs:
        subs[topic] = []

    subs[topic].append(callback)


def pub(topic, payload):
    if topic in subs:
        for callback in subs[topic]:
            callback(topic, payload)
    else:
        for sub in subs:
            if sub.endswith('#') and topic.startswith(sub[0:len(sub)-1]):
                for callback in subs[sub]:
                    callback(topic, payload)
                return


server = PubSubLocalPipeTelemetryServer("topic", pub, sub)

logger = TelemetryLogger('test-stream')
logger.telemetry_client = PubSubTelemetryLoggerClient("topic", pub, sub)
logger.addByte('byte')
logger.addDouble('double')
logger.init()

client = PubSubTelemetryClient("topic", pub, sub)

logger.log(1, 10, 2.0)
logger.log(2, 20, 2.1)
logger.log(3, 30, 2.2)


stream_names = None
test_stream = None
test_stream_data = None


def receive_streams(streams):
    global stream_names
    stream_names = streams


def receive_test_stream(received_stream):
    global test_stream
    test_stream = received_stream


def receive_test_stream_data(received_records):
    global test_stream_data
    test_stream_data = received_records


client.getStreams(receive_streams)

while stream_names is None:
    time.sleep(0.01)

print("Received streams " + str(stream_names))

client.getStreamDefinition('test-stream', receive_test_stream)

while test_stream is None:
    time.sleep(0.01)

print("Received stream def " + str(test_stream.toJSON()))

time.sleep(1)

client.retrieve(test_stream, 0, time.time(), receive_test_stream_data)

while test_stream_data is None:
    time.sleep(0.01)

print("Received data " + str(test_stream_data))

