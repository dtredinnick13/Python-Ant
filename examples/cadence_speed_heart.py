# ANT - Cadence, Speed Sensor AND Heart Rate Monitor - Example
#
# Copyright (c) 2012, Gustav Tiger <gustav@tiger.name>
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

from __future__ import absolute_import, print_function

from ant.easy.node import Node
from ant.easy.channel import Channel
from ant.base.message import Message

import binascii
import paho.mqtt.client as paho
import logging
import struct
import threading
import sys

NETWORK_KEY= [0xb9, 0xa5, 0x21, 0xfb, 0xbd, 0x72, 0xc3, 0x45]
broker="192.168.118.35"
port=1883

class Monitor():
    def __init__(self):
        self.heartrate = "n/a";
        self.cadence = "n/a";
        self.speed = "n/a";
        self.mqttclient = paho.Client("control1")
        self.mqttclient.connect(broker,port)

    def on_data_heartrate(self, data):
        #self.heartrate = str(data[7])
        #print ("heart rate ", data)
        publishstr = '{"deviceType":120,"message":"'+format(data[0],'02x')+format(data[1],'02x')+format(data[2],'02x')+format(data[3],'02x')+format(data[4],'02x')+format(data[5],'02x')+format(data[6],'02x')+format(data[7],'02x')+'"}'
        print(publishstr)
        self.mqttclient.publish("org.teamtibco.bikesensor.rawdata",publishstr)
        #self.display()

    def on_data_cadence(self, data):
        #self.cadence = str(data[3]*256 + data[2])
        #self.speed = str(data[7]*256 + data[6])
        #self.cadence = str(data[7]*256 + data[6])
        #print ("cadence ", data)
        publishstr = '{"deviceType":122,"message":"'+format(data[0],'02x')+format(data[1],'02x')+format(data[2],'02x')+format(data[3],'02x')+format(data[4],'02x')+format(data[5],'02x')+format(data[6],'02x')+format(data[7],'02x')+'"}'
        print(publishstr)
        self.mqttclient.publish("org.teamtibco.bikesensor.rawdata",publishstr)
        #self.display()

    def on_data_speed(self, data):
        #self.cadence = str(data[3]*256 + data[2])
        #self.speed = str(data[7]*256 + data[6])
        #self.speed = str(data[7]*256 + data[6])
        #print ("speed ", data)
        publishstr = '{"deviceType":123,"message":"'+format(data[0],'02x')+format(data[1],'02x')+format(data[2],'02x')+format(data[3],'02x')+format(data[4],'02x')+format(data[5],'02x')+format(data[6],'02x')+format(data[7],'02x')+'"}'
        print(publishstr)
        self.mqttclient.publish("org.teamtibco.bikesensor.rawdata",publishstr)
        #self.display()
    
    def on_data_power(self, data):
        #print ("power ", data)
        publishstr = '{"deviceType":11,"message":"'+format(data[0],'02x')+format(data[1],'02x')+format(data[2],'02x')+format(data[3],'02x')+format(data[4],'02x')+format(data[5],'02x')+format(data[6],'02x')+format(data[7],'02x')+'"}'
        print(publishstr)
        self.mqttclient.publish("org.teamtibco.bikesensor.rawdata",publishstr)
        #self.display()

    def display(self):
        string = "Hearthrate: " + self.heartrate + " Pedal revolutions: " + self.cadence + " Wheel revolutions: " + self.speed

        sys.stdout.write(string)
        sys.stdout.flush()
        sys.stdout.write("\b" * len(string))


def main():
    # logging.basicConfig()

    monitor = Monitor()

    node = Node()
    node.set_network_key(0x00, NETWORK_KEY)

    channel = node.new_channel(Channel.Type.BIDIRECTIONAL_RECEIVE)
    channel.on_broadcast_data = monitor.on_data_heartrate
    channel.on_burst_data = monitor.on_data_heartrate
    channel.set_period(8070)
    channel.set_search_timeout(12)
    channel.set_rf_freq(57)
    channel.set_id(0, 120, 0)

    channel_cadence = node.new_channel(Channel.Type.BIDIRECTIONAL_RECEIVE)
    channel_cadence.on_broadcast_data = monitor.on_data_cadence
    channel_cadence.on_burst_data = monitor.on_data_cadence
    channel_cadence.set_period(8102)
    channel_cadence.set_search_timeout(30)
    channel_cadence.set_rf_freq(57)
    channel_cadence.set_id(0, 122, 0)

    channel_speed = node.new_channel(Channel.Type.BIDIRECTIONAL_RECEIVE)
    channel_speed.on_broadcast_data = monitor.on_data_speed
    channel_speed.on_burst_data = monitor.on_data_speed
    channel_speed.set_period(8118)
    channel_speed.set_search_timeout(30)
    channel_speed.set_rf_freq(57)
    channel_speed.set_id(0, 123, 0)

    channel_power = node.new_channel(Channel.Type.BIDIRECTIONAL_RECEIVE)
    channel_power.on_broadcast_data = monitor.on_data_power
    channel_power.on_burst_data = monitor.on_data_power
    channel_power.set_period(8182)
    channel_power.set_search_timeout(30)
    channel_power.set_rf_freq(57)
    channel_power.set_id(0, 11, 0)

    try:
        channel.open()
        channel_cadence.open()
        channel_speed.open()
        channel_power.open()
        node.start()
    finally:
        node.stop()
    
if __name__ == "__main__":
    main()

