#
# Copyright 2019 Games Creators Club
#
# MIT License
#

import paho.mqtt.client as mqtt
import random
import re
import sys
import threading
import time
import traceback


from telemetry.telemetry_logger import TelemetryLogger, LocalPipeTelemetryLoggerDestination, PubSubTelemetryLoggerClient
from telemetry.telemetry_client import PubSubTelemetryClient


class MQTTLocalPipeTelemetryLogger(TelemetryLogger):
    def __init__(self, stream_name, host="localhost", port=1883, topic='telemetry'):
        self.mqtt = MQTTWrapper(host, port)
        super(MQTTLocalPipeTelemetryLogger, self).__init__(stream_name,
                                                           destination=LocalPipeTelemetryLoggerDestination(),
                                                           telemetry_client=PubSubTelemetryLoggerClient(topic, self.mqtt.publish, self.mqtt.subscribe))

    def init(self):
        while not self.mqtt.isConnected():
            self.mqtt.loop(0.02)

        super(MQTTLocalPipeTelemetryLogger, self).init()
        while not self.stream_ready and self.registration_error == 0:
            self.mqtt.loop(0.02)


class MQTTTelemetryClient(PubSubTelemetryClient):
    def __init__(self, host="localhost", port=1883, topic='telemetry'):
        self.mqtt = MQTTWrapper(host, port)
        super(MQTTTelemetryClient, self).__init__(topic, self.mqtt.publish, self.mqtt.subscribe)


class MQTTWrapper:
    def __init__(self, host="localhost", port=1883, auto_init=True):

        self.client = None

        self.host = host
        self.port = port

        self.name = "telemetry-server-" + str(random.randint(10000, 99999))
        self._subscribers = []
        self._regexToLambda = {}
        self._received = False
        self.connected = False

        if auto_init:
            self.init()

    def init(self, waitToConnect=True):

        self.client = mqtt.Client(self.name)

        self.client.on_disconnect = self._onDisconnect
        self.client.on_connect = self._onConnect
        self.client.on_message = self._onMessage

        if self.host is not None:
            self._connect()

        if waitToConnect:
            print("    " + self.name + " waiting to connect to broker...")
            while not self.connected:
                self.loop(0.02)
            print("    " + self.name + " connected to broker.")

    def _connect(self):
        self.connected = False

        if self.client is not None:
            try:
                self.client.disconnect()
            except:
                pass

        self.client.connect_async(self.host, self.port, 60)
        thread = threading.Thread(target=self._reconnect)
        thread.daemon = True
        thread.start()

    def _onDisconnect(self, mqttClient, data, rc):
        self._connect()

    def _onConnect(self, mqttClient, data, flags, rc):
        if rc == 0:
            self.connected = True
            for subscriber in self._subscribers:
                mqttClient.subscribe(subscriber, 0)

        else:
            print("ERROR: Connection returned error result: " + str(rc))
            sys.exit(rc)

    def _onMessage(self, mqttClient, data, msg):
        global _received

        _received = True

        topic = msg.topic

        try:
            for regex in self._regexToLambda:
                matching = regex.match(topic)
                if matching:
                    method = self._regexToLambda[regex]

                    method(topic, msg.payload)
                    return

        except Exception as ex:
            print("ERROR: Got exception in on message processing; " + str(ex) + "\n" + ''.join(traceback.format_tb(ex.__traceback__)))

    def _reconnect(self):
        try:
            self.client.reconnect()
        except:
            pass

    def publish(self, topic, message):
        if self.connected:
            self.client.publish(topic, message)

    def subscribe(self, topic, method):
        self._subscribers.append(topic)
        regexString = "^" + topic.replace("+", "([^/]+)").replace("#", "(.*)") + "$"
        regex = re.compile(regexString)
        self._regexToLambda[regex] = method

        if self.connected:
            self.client.subscribe(topic, 0)

    def isConnected(self):
        return self.connected

    def sleep(self, deltaTime):
        self.loop(self, deltaTime)

    def loop(self, deltaTime, inner=None):

        currentTime = time.time()

        self._received = False
        self.client.loop(0.0005)  # wait for 0.5 ms

        until = currentTime + deltaTime
        while currentTime < until:
            if self._received:
                self._received = False
                self.client.loop(0.0005)  # wait for 0.1 ms
                currentTime = time.time()
            else:
                time.sleep(0.002)  # wait for 2 ms
                currentTime = time.time()
                if currentTime + 0.0005 < until:
                    self.client.loop(0.0005)  # wait for 0.1 ms
                    currentTime = time.time()

    def forever(self, deltaTime, outer=None, inner=None):

        currentTime = time.time()
        nextTime = currentTime

        while True:

            nextTime = nextTime + deltaTime
            try:
                if outer is not None:
                    outer()
            except BaseException as ex:
                print("ERROR: Got exception in main loop; " + str(ex) + "\n" + ''.join(traceback.format_tb(ex.__traceback__)))

            currentTime = time.time()

            sleepTime = nextTime - currentTime
            if sleepTime < 0.002:
                nextTime = currentTime

                self._received = False
                self.client.loop(0.0005)  # wait for 0.1 ms
                count = 10  # allow at least 5 messages
                while count > 0 and self._received:
                    self._received = True
                    count -= 1
                    self.client.loop(0.0005)  # wait for 0.1 ms

            else:
                self.loop(sleepTime, inner=inner)
