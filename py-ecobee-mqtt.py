#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
    File name: py-ecobee-mqtt.py
    Author: Derek Rowland
    Date created: 2020/08/03
    Date last modified: 2020/08/03
    Python Version: 3.8.5
'''

'''
******* Imports 
'''
import requests
import os, sys, signal, time
from configparser import ConfigParser
import paho.mqtt.client as mqtt
from pyecobee import * #https://github.com/sfanous/Pyecobee

import shelve
from datetime import datetime
import pandas as pd
import pytz
from six.moves import input
from datetime import datetime, timezone
import json

import logging
from logging.handlers import TimedRotatingFileHandler

'''
******* Header Vars
'''

__author__ = "Derek Rowland"
__copyright__ = "Copyright 2020"
__credits__ = ["Derek Rowland"]
__license__ = "GPL"
__version__ = "0.0.1"
__maintainer__ = "Derek Rowland"
__email__ = "gx1400@gmail.com"
__status__ = "Development"

'''
******* Global vars
'''
logger = logging.getLogger(__name__)
mqttAddr = 'not loaded'
mqttPort = -1
mqttTopic = 'not loaded'
tokenEcobee = 'not loaded'
nameEcobee = 'not loaded'
dbFile = 'not loaded'
ecobee_service = None
terminate = False
client = None

'''
******* Functions
'''
def main():
    global client

    # Read Config File parameters
    read_config()

    logger_setup()

    #try to connect to ecobee
    ecobee_connect()
    
    # Connect to Mqtt
    client = mqtt.Client()
    client.on_connect = mqtt_on_connect
    client.on_message = mqtt_on_message

    try:
        logger.info('Attempting to connect to mqtt server: ' + mqttAddr + 
            ':' + str(mqttPort))
        client.connect(mqttAddr, mqttPort, 60)
    except:
        logger.error('failed to connect to mqtt.... aborting script')
        sys.exit()

    signal.signal(signal.SIGINT, signal_handler)

    logger.info('Starting loop...')

    #client.loop_forever()
    client.loop_start()
    loopct = 0

    try:
        while True:
            if terminate:
                mqtt_endloop()
                break
        
            if (loopct >= 180):
                logger.info('Start of loop')
                try:
                    ecobee_mqtt()
                except requests.ConnectionError:
                    logger.error('Connection error!')
                except requests.Timeout:
                    logger.error('Timeout error')
                except requests.RequestException as e:
                    raise SystemExit(e)
                loopct = 0
            else:
                time.sleep(1)
                loopct += 1
    except:
        logger.error("Unexpected error:", sys.exc_info()[0])
        
    
    logger.info('Exiting program')

def ecobee_authorize(ecobee_service):
    authorize_response = ecobee_service.authorize()
    logger.debug('AutorizeResponse returned from ecobee_service.authorize():\n{0}'.format(
        authorize_response.pretty_format()))

    persist_to_shelf('pyecobee_db', ecobee_service)

    logger.info('Please goto ecobee.com, login to the web portal and click on the settings tab. Ensure the My '
                'Apps widget is enabled. If it is not click on the My Apps option in the menu on the left. In the '
                'My Apps widget paste "{0}" and in the textbox labelled "Enter your 4 digit pin to '
                'install your third party app" and then click "Install App". The next screen will display any '
                'permissions the app requires and will ask you to click "Authorize" to add the application.\n\n'
                'After completing this step please hit "Enter" to continue.'.format(
        authorize_response.ecobee_pin))
    input()

def ecobee_checktokens():
    now_utc = datetime.now(pytz.utc)
    if now_utc > ecobee_service.refresh_token_expires_on:
        ecobee_authorize(ecobee_service)
        ecobee_request_tokens(ecobee_service)
    elif now_utc > ecobee_service.access_token_expires_on:
        token_response = ecobee_refresh_tokens(ecobee_service)

# function for connecting to ecobee service
def ecobee_connect():
    global dbFile
    global ecobee_service

    try:
        thisfolder = os.path.dirname(os.path.abspath(__file__))
        dbFile = os.path.join(thisfolder, 'db', 'pyecobee_db')
        pyecobee_db = shelve.open(dbFile, protocol=2)
        ecobee_service = pyecobee_db[nameEcobee]
    except KeyError:
        ecobee_service = EcobeeService(thermostat_name=nameEcobee, application_key=tokenEcobee)
    finally:
        pyecobee_db.close()

    if ecobee_service.authorization_token is None:
        ecobee_authorize(ecobee_service)
    else:
        logger.debug('auth token: ' + ecobee_service.authorization_token)

    if ecobee_service.access_token is None:
        ecobee_request_tokens(ecobee_service)
    else:
        logger.debug('access token: ' + ecobee_service.access_token)
    
    ecobee_checktokens()

def ecobee_mqtt():

    selection = Selection(selection_type=SelectionType.REGISTERED.value, selection_match='', include_alerts=False,
                      include_device=False, include_electricity=False, include_equipment_status=True,
                      include_events=False, include_extended_runtime=False, include_house_details=False,
                      include_location=False, include_management=False, include_notification_settings=False,
                      include_oem_cfg=False, include_privacy=False, include_program=False, include_reminders=False,
                      include_runtime=True, include_security_settings=False, include_sensors=True,
                      include_settings=True, include_technician=False, include_utility=False, include_version=False,
                      include_weather=False)
    
    # proactively try to refresh tokens, but if we hit the sweet spot, we'll try to react
    ecobee_checktokens()
    
    #get thermostat data
    try:
        thermostat_response = ecobee_service.request_thermostats(selection)
        #logger.debug(thermostat_response.pretty_format())
    except EcobeeApiException as e:
        if e.status_code == 14:
            token_response = ecobee_service.refresh_tokens()

    assert thermostat_response.status.code == 0, 'Failure while executing request_thermostats:\n{0}'.format(
        thermostat_response.pretty_format())
    utc_time = datetime.now(timezone.utc)
    ts = pd.Timestamp(utc_time)
    epoch5min = int(ts.round(freq='5min').timestamp())

    #testing extracting data from json obj
    # docs here: https://pydoc.net/pyecobee/1.2.0/
    for item in thermostat_response.thermostat_list:
        
        # iterate through 'sensors' for temp/humidity/occupancy data
        for sensor in item.remote_sensors:
            logger.debug(sensor)
            roomname = sensor.name.replace(' ','-').lower()
            topicname = mqttTopic + 'sensor/' + roomname + '/'
            
            for cap in sensor.capability:
                pubtopic = topicname + cap.type
                logger.debug(pubtopic)

                parsedValue = cap.value

                if (cap.value == 'unknown'):
                    parsedValue = int(-1)
                elif (cap.type == 'temperature'):
                    parsedValue = int(cap.value) / 10
                elif (cap.type == "humidity"): 
                    parsedValue = int(cap.value)
                elif (cap.type == "occupancy"):
                    parsedValue = int(cap.value == 'true')
                else:
                    parsedValue = cap.value

                msg = {
                    'thermostat' : item.name,
                    'room': roomname,
                    'code': sensor.code,
                    'type': cap.type,
                    'value': parsedValue,
                    'timestamp': epoch5min
                }

                logger.debug(msg)
                client.publish(pubtopic, json.dumps(msg), 0, False)

        #log equipment status
        eStatusList = item.equipment_status.split(',')
        logger.debug('Equipment status: ' + json.dumps(eStatusList))
        msg = {
            'name': item.name,
            'fan': int('fan' in eStatusList),
            'compCool1': int('compCool1' in eStatusList),
            'compCool2': int('compCool2' in eStatusList),
            'auxHeat1': int('auxHeat1' in eStatusList),
            'auxHeat2': int('auxHeat2' in eStatusList),
            'auxHeat3': int('auxHeat3' in eStatusList),
            'auxHotWater': int('auxHotWater' in eStatusList),
            'compHotWater': int('compHotWater' in eStatusList),
            'dehumidifier': int('dehumidifier' in eStatusList),
            'economizer': int('economizer' in eStatusList),
            'heatPump': int('heatPump' in eStatusList),
            'heatPump2': int('heatPump2' in eStatusList),
            'heatPump3': int('heatPump3' in eStatusList),
            'humidifier': int('humidifier' in eStatusList),
            'ventilator': int('ventilator' in eStatusList),
            'timestamp': epoch5min
        }
        statusMsg = json.dumps(msg)
        logger.debug(statusMsg)
        statusTopic = mqttTopic  + 'runningStatus'
        client.publish(statusTopic, statusMsg, 0, False)


        #log runtime information
        logger.debug(item.runtime)

        mode = item.settings.hvac_mode
        logger.debug('mode: ' + mode)
        setH = item.runtime.desired_cool /10
        setL = item.runtime.desired_heat /10
        setpointlow = 0
        setpointhigh = 0
        if (mode == 'auto'):
            setpointlow = setL
            setpointhigh = setH
        elif (mode == 'cool'):
            setpointlow = None
            setpointhigh = setH
        elif (mode == 'heat'):
            setpointlow = setL
            setpointhigh = None
        elif (mode == 'off'):
            setpointlow = None
            setpointhigh = None
        elif (mode == 'auxHeatOnly'): #how to handle?
            setpointlow = setH
            setpointhigh = None
        else:
            setpointlow = None
            setpointhigh = None

        msg = {
            'name': item.name,
            'desiredHeat': item.runtime.desired_heat /10,
            'desiredCool': item.runtime.desired_cool /10,
            'desiredHum': item.runtime.desired_humidity ,
            'desiredDeHum': item.runtime.desired_dehumidity,
            'desiredFanMode': item.runtime.desired_fan_mode,
            'setpointlow' : setpointlow,
            'setpointhigh': setpointhigh,
            'actualTemp' : item.runtime.actual_temperature /10,
            'timestamp': epoch5min
        }
        rtMsg = json.dumps(msg)
        logger.debug(rtMsg)
        rtTopic = mqttTopic + 'setpoints'
        client.publish(rtTopic, rtMsg, 0, False)

def donothing():
    nothing = None   

# function for refreshing token from ecobee
def ecobee_refresh_tokens(ecobee_service):
    token_response = ecobee_service.refresh_tokens()
    logger.debug('TokenResponse returned from ecobee_service.refresh_tokens():\n{0}'.format(
        token_response.pretty_format()))

    persist_to_shelf(dbFile, ecobee_service)

# function for requesting token from ecobee
def ecobee_request_tokens(ecobee_service):
    token_response = ecobee_service.request_tokens()
    logger.debug('TokenResponse returned from ecobee_service.request_tokens():\n{0}'.format(
        token_response.pretty_format()))

    persist_to_shelf(dbFile, ecobee_service)


def logger_setup():
    global logger
    thisfolder = os.path.dirname(os.path.abspath(__file__))
    logFile = os.path.join(thisfolder, 'log', 'logger.log')

    formatter = logging.Formatter('%(asctime)s %(name)-18s %(levelname)-8s %(message)s')
    handler = TimedRotatingFileHandler(logFile,
                                       when="d",
                                       interval=1,
                                       backupCount=7)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    logger.setLevel(logging.DEBUG)

def mqtt_endloop():
    client.loop_stop()
    logger.info('loop stopped!')
    client.disconnect()

# call back for client connection to mqtt
def mqtt_on_connect(client, userdata, flags, rc):
    logger.info('Mqtt Connection result code: ' + str(rc))

    # subscribing in on_connect means if we lose the connection and 
    # reconnect then subscriptions will be renewed
    client.subscribe('$SYS/#')

# call back for when a public message is received by the server
def mqtt_on_message(client, userdata, msg):
    donothing()

# function for writing to ecobee persistent db
def persist_to_shelf(file_name, ecobee_service):
    pyecobee_db = shelve.open(file_name, protocol=2)
    pyecobee_db[ecobee_service.thermostat_name] = ecobee_service
    pyecobee_db.close()

# function for reading the config.cfg file to set global operation params
def read_config():
    parser = ConfigParser()
    thisfolder = os.path.dirname(os.path.abspath(__file__))
    configfile = os.path.join(thisfolder, 'config.cfg')
    parser.read(configfile, encoding=None)

    global mqttAddr, mqttPort, mqttTopic, tokenEcobee

    mqttAddr = parser.get('mqtt', 'ipaddr').strip('\'')
    mqttPort = parser.getint('mqtt', 'port')
    mqttTopic = parser.get('mqtt', 'topic').strip('\'')

    tokenEcobee = parser.get('ecobee', 'token').strip('\'')
    nameEcobee = parser.get('ecobee', 'thermostatname').strip('\'')

def signal_handler(signum,frame):
    global terminate
    terminate = True

# main function call
if __name__ == "__main__":
    main()