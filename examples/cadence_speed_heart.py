# ANT - Team TIBCO-SVB Sensor Connectivity Script
#
# Copyright (c) 2019, TIBCO Software Inc
#
# 

from __future__ import absolute_import, print_function

from ant.easy.node import Node
from ant.easy.channel import Channel
from ant.base.message import Message

import binascii
import paho.mqtt.client as paho
import json
import logging
import struct
import threading
import time
import sys

# This function provides the logic for handling the restart command
# that would be received from MQTT 
def on_ext_message(client, userdata, message):
    print("Message received: " + message.payload)
    commandDict = json.loads(message.payload)
    commandstr = commandDict["command"]
    print("Command: " + commandstr)

    if commandstr == "restart":
        node.stop()
        print ("node stopped")
        channel = ChannelMaster()
        print ("Channel defined")
        monitor = Monitor()
        print ("Monitor defined")
        channel.initialize(monitor)
        channel.openChannels()
        node.start()
    else:
        print("Invalid Command Received")

# This function validates the connection to the MQTT broker.  This 
# seems to be the only way to really tell if the connection was
# successful.
def on_ext_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to broker")      
        global Connected
        Connected = True
    else:
        print("Connected failed")

# Set up logging
logger = logging.getLogger("ant.easy.csh")
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('csh.log', 'a', None, False)
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)
logging.basicConfig(level=logging.INFO)
logger.info('start run')


# This key is as deifned by the Ant Specification
NETWORK_KEY= [0xb9, 0xa5, 0x21, 0xfb, 0xbd, 0x72, 0xc3, 0x45]

# Create an instance of node.  This needs to be performed here in order to have node in scope for
# the command processing functions that are to be developed.
node = Node()

# MQTT Connection information for local in-pi communciation
localMQTTbroker="localhost"
localMQTTport=1883

# MQTT Connection information for external communication
extMQTTBroker="localhost"
extMQTTPort=1883

# Monitor sets up call back functions to handle messages associated with each of the sensor types.
# To add more follow the pattern for each of these functions.  Also, to have the function get called
# there must be a corresponding section in the ChannelMaster initialize function.
#
# Each one of the on_data functions provides the callback function for processing the paticular type 
# of sensor message.
class Monitor():
    def __init__(self):
        self.localmqttclient = paho.Client("localClient")
        self.localmqttclient.connect(localMQTTbroker,localMQTTport)

    def on_data_heartrate(self, data):
        publishstr = '{"deviceType":120,"message":"'+format(data[0],'02x')+format(data[1],'02x')+format(data[2],'02x')+format(data[3],'02x')+format(data[4],'02x')+format(data[5],'02x')+format(data[6],'02x')+format(data[7],'02x')+'"}'
        print(publishstr)
        logger.info(publishstr)
        self.localmqttclient.publish("org.teamtibco.bikesensor.rawdata",publishstr)

    def on_data_cadence(self, data):
        publishstr = '{"deviceType":122,"message":"'+format(data[0],'02x')+format(data[1],'02x')+format(data[2],'02x')+format(data[3],'02x')+format(data[4],'02x')+format(data[5],'02x')+format(data[6],'02x')+format(data[7],'02x')+'"}'
        print(publishstr)
        logger.info(publishstr)
        self.localmqttclient.publish("org.teamtibco.bikesensor.rawdata",publishstr)

    def on_data_speed(self, data):
        publishstr = '{"deviceType":123,"message":"'+format(data[0],'02x')+format(data[1],'02x')+format(data[2],'02x')+format(data[3],'02x')+format(data[4],'02x')+format(data[5],'02x')+format(data[6],'02x')+format(data[7],'02x')+'"}'
        print(publishstr)
        logger.info(publishstr)
        self.localmqttclient.publish("org.teamtibco.bikesensor.rawdata",publishstr)
    
    def on_data_power(self, data):
        publishstr = '{"deviceType":11,"message":"'+format(data[0],'02x')+format(data[1],'02x')+format(data[2],'02x')+format(data[3],'02x')+format(data[4],'02x')+format(data[5],'02x')+format(data[6],'02x')+format(data[7],'02x')+'"}'
        print(publishstr)
        logger.info(publishstr)
        self.localmqttclient.publish("org.teamtibco.bikesensor.rawdata",publishstr)

# ChannelMaster provides the intialization for each of the monitor channels.  Currently heartrate, cadence, power, and speed are
# supported.  Any valid ANT channel can be added by following the same pattern. It also provides the function that opens the 
# channels.
class ChannelMaster():
    def __init__(self):
        print("Initializing Channel Master")

    def initialize(self, Monitor):
        try:
           self.channel_heartrate = node.new_channel(Channel.Type.BIDIRECTIONAL_RECEIVE)
           self.channel_heartrate.on_broadcast_data = Monitor.on_data_heartrate
           self.channel_heartrate.on_burst_data = Monitor.on_data_heartrate
           self.channel_heartrate.set_period(8070)
           self.channel_heartrate.set_search_timeout(60)
           self.channel_heartrate.set_rf_freq(57)
           self.channel_heartrate.set_id(0, 120, 0)
           print("Initialized Heartrate")
        except Exception as ex:
            print(ex)

        self.channel_cadence = node.new_channel(Channel.Type.BIDIRECTIONAL_RECEIVE)
        self.channel_cadence.on_broadcast_data = Monitor.on_data_cadence
        self.channel_cadence.on_burst_data = Monitor.on_data_cadence
        self.channel_cadence.set_period(8102)
        self.channel_cadence.set_search_timeout(60)
        self.channel_cadence.set_rf_freq(57)
        self.channel_cadence.set_id(0, 122, 0)
        print("Initialized Cadence")

        self.channel_speed = node.new_channel(Channel.Type.BIDIRECTIONAL_RECEIVE)
        self.channel_speed.on_broadcast_data = Monitor.on_data_speed
        self.channel_speed.on_burst_data = Monitor.on_data_speed
        self.channel_speed.set_period(8118)
        self.channel_speed.set_search_timeout(60)
        self.channel_speed.set_rf_freq(57)
        self.channel_speed.set_id(0, 123, 0)
        print("Initialized Speed")

        self.channel_power = node.new_channel(Channel.Type.BIDIRECTIONAL_RECEIVE)
        self.channel_power.on_broadcast_data = Monitor.on_data_power
        self.channel_power.on_burst_data = Monitor.on_data_power
        self.channel_power.set_period(8182)
        self.channel_power.set_search_timeout(60)
        self.channel_power.set_rf_freq(57)
        self.channel_power.set_id(0, 11, 0)
        print("Initialized Power")

    def openChannels(self):
        self.channel_heartrate.open()
        self.channel_cadence.open()
        self.channel_speed.open()
        self.channel_power.open()

# This class handles control messages coming into the script.  The goal is to have these do shutdown, restart, 
# and potentially even change logging levels.  This is currently incomplete.  The callback functions are 
# provided eariler in the code.
class CommunicationHandler():
    def __init__(self):
        print("Initializing external communications")
        self.externalmqttclient = paho.Client("externalClient")
        self.externalmqttclient.on_connect = on_ext_connect
        self.externalmqttclient.on_message = on_ext_message
        self.externalmqttclient.username_pw_set("dtredinnick", password="Tibco.123")
        self.externalmqttclient.connect(extMQTTBroker,extMQTTPort)
        self.externalmqttclient.loop_start()
        print("Connected to External MQTT Broker!")

    def subscribe_ext_message(self, subjectName):
        self.externalmqttclient.subscribe(subjectName)
        print("Subscribed to " + subjectName)


def main():
    # Instantiate an instance of the Monitor class.  The Monitor class owns the MQTT connection, and 
    # provides the callback functions for the various sensor messages
    monitor = Monitor()
    
    # Set the network key for the node
    node.set_network_key(0x00, NETWORK_KEY)
    
    # Create and initialze the channel.  This must be performed before the node can be started.
    channel = ChannelMaster()
    channel.initialize(monitor)
    
    # Create an instance of the communication handler and subscribe to the MQTT control messages.  
    commHandler = CommunicationHandler()
    commHandler.subscribe_ext_message("org.teamtibco.antdevice.control")

    # Within the try block perform the following steps:
    #  1) open the channels
    #  2) start the node (This creates a listen loop, so the remaining steps won't get executed until the 
    #     node is stopped.)
    #  3) create an infinite loop in order to keep listening for MQTT control messages since at this point
    #     we can no longer take advatage of the node loop.  There may be a more elegant way to do this, 
    #     but for now this works.  This loop is really for future development of the control functions.
    #
    # There are two exception handlers.  The first one looks for a Ctrl-C and then initiates a graceful shutdown.
    # The second one catches any other errors that may have occurred and then does a graceful shutdown.  
    # 
    # The finally code is what executes the shutdown.
    try:
        channel.openChannels()
        print("Channels open!")
        node.start()
        while True:
          time.sleep(0.5)

        # print("Node Started!")
    except KeyboardInterrupt:
        print("Received Ctrl-C")
    except:
        print("Channel Failure")
    finally:
        print("Shutting Down Node")
        logger.info("Shutting Down Node")
        node.stop()

# No idea what this last line does.      
if __name__ == "__main__":
    main()

