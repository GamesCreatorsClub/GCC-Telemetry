#
# Copyright 2019 Games Creators Club
#
# MIT License
#

from telemetry.telemetry_mqtt import MQTTWrapper
from telemetry_server import PubSubLocalPipeTelemetryServer


class MQTTLocalPipeTelemetryServer(PubSubLocalPipeTelemetryServer):
    def __init__(self, host="localhost", port=1883, topic="telemetry"):
        self.mqtt = MQTTWrapper(host, port, topic)

        super(MQTTLocalPipeTelemetryServer, self).__init__(topic, self.mqtt.publish, self.mqtt.subscribe)

    def waitAndProcess(self, waitTime=0.02):  # 50 times a second by default
        self.mqtt.loop(waitTime)

    def runForever(self, waitTime=0.02, outer=None):  # 50 times a second by default
        self.mqtt.forever(waitTime, outer)
