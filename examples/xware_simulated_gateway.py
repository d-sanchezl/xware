# MQTT to OM2M gateway device client with simulated aquisition
# See Github repo (github.com/d-sanchezl/xware) for license details

# Make sure you review the "USER PARAMETERS" and "DATA AQUISITION FUNCTION"
# sections before executing this code.

# Import necessary packages
import paho.mqtt.client as mqtt # MQTT
import time # To time sending intervals

# Import xware libraries
from xware_lib_functions import *
from xware_lib_om2m import *
import xware_globals



# ==================================================================
# USER PARAMETERS:
# Change these to your liking

# Sampling Frequency [Hz]
F = 1000

# Time length of a single sampling [s]
t = 2

# Time interval between samplings [s]
T = 5

# Sensor to real value scaling
valueConversion = 1/0.00989

# Device identifier
# * Example: 'induction_motor_1'
deviceTag = 'induction_motor'

# Sensor identifiers
# Example: 'x_accel'

# * If there are multiple sensors, separate them with commas
# * Example: 'x_accel,y_accel,z_accel'

sensorTag = 'x_accel,y_accel,z_accel'

# MQTT address, change to target (server) IP address
brokerAddress = '192.138.6.70'


# =================================
# ADVANCED PARAMETERS:
# Do not change these unless you know what you are doing

# MQTT topic name
deviceName = deviceTag

# OM2M names
serverCSE = 'in-cse'
serverName = 'in-name'
containerName = 'sampling'
eventsContName = 'events'
authOM2M = 'admin:admin' # user:password

# Time between message receipt verification
waitTime = 0.1
# Time between message sent retries
retryWaitTime = 1
# Maximum time before message sending stops and code exits
maxWaitTime = 6




# ==================================================================
# DATA AQUISITION FUNCTION:

# Place your data aquisition code here. You may use as many libraries, functions
# and lines of code as you like. Simply make sure that you create a function
# "getValueFromSensor()" that retuns a sensor value or values when it is called.

# The XWare code will run the "getValueFromSensor()" function whenever it needs
# a value or values from the sensor(s), meaning F times per second.

# If you collect data from a single sensor, a valid string that
# "getValueFromSensor()" might return is:

#  '2.99808'

# If you collect data from multiple sensors, simply place all the values
# in a single string separated by commas without spaces. For example:

#  '2.99808,-11.24914,-0.77331'

# Note 1: If you have multiple sensors, make sure you define the sensorTag
# string (USER PARAMETERS section) correctly.

# Note 2: The getValueFromSensor() function does not take any inputs. If you
# require any inputs, use python globals.

# ==========
# Code:

# This example code reads data from a file or files in a "samplesLocation"
# folder. The timestamps in the files are ignored, and only sensor values
# are read. An example file that can be read this way ('simulated_input.txt')
# can be found in the repo.

# Create and manage directories
import os

# Simulated acquisition parameters
samplesLocation = "/home/pi/Documents/simulated_acquisition"

# Create file list
fileList = [(samplesLocation+"/"+i) for i in os.listdir(samplesLocation)]
currentFileIndex = 0
currentLineIndex = 0
lineList = []

# Read next value from the txt files
def readOneValue():
    global fileList
    global currentFileIndex
    global currentLineIndex
    global lineList
    # Read new file if necessary
    if not(lineList):
        file = open(fileList[currentFileIndex], 'r')
        contents = file.read()
        lineList = contents.splitlines()
        file.close()
    # Read the corresponding line
    currentLine = lineList[currentLineIndex]
    # Cycle to next valid line/file
    currentLineIndex += 1
    if currentLineIndex >= len(lineList):
        currentFileIndex += 1
        currentLineIndex = 0
        lineList = []
        if currentFileIndex >= len(fileList):
            currentFileIndex = 0
    # Extract value from full string
    tabPos = currentLine.find('\t')
    if tabPos == -1:
        tabPos = currentLine.find(',')
    valueStr = currentLine[tabPos+1:]
    return valueStr

# Read 3 values to simulate data from 3 sensors
def getValueFromSensor():
    x = readOneValue()
    y = readOneValue()
    z = readOneValue()
    return x + ',' + y + ',' + z





# ==================================================================
# XWARE CODE

# This is the XWare code.
# You should not have to change anything beyond this point.


#====================
# SET UP FLAGS AND VARIABLES

xware_globals.newMessage = None
xware_globals.messageString = ''
messageIndex = 0


#====================
# Time parameters
deltaTime = 1/F
sampleWaitTime = deltaTime/10


#====================
# OM2M SETTINGS
topicReq = '/oneM2M/req/'+deviceName+'/'+serverCSE+'/json'
topicResp = '/oneM2M/resp/'+serverCSE+'/'+deviceName+'/json'
to_app  = '/'+serverCSE+'/'+serverName
to_cont = '/'+serverCSE+'/'+serverName+'/'+deviceName
to_data  = '/'+serverCSE+'/'+serverName+'/'+deviceName+'/'+containerName
to_events  = '/'+serverCSE+'/'+serverName+'/'+deviceName+'/'+eventsContName


#====================
# WAIT FOR OPERATOR INPUT
print('Selected sampling frequency is ' + str(F) + ' Hz')
print('Selected sampling time is ' + str(t) + ' s')
print('Selected sampling period is ' + str(T) + ' s')
print('Make sure that XWare Server is active before starting!') #!!!
input('Press Return to begin...')
print('')


#====================
# MQTT CONNECT TO OM2M
client = mqtt.Client(deviceName, userdata = topicResp)
client.on_message = onMessageMQTT
client.on_connect = onConnectMQTT
client.connect(brokerAddress)
client.loop_start()


# ===========
# Check if OM2M application exists via MQTT
payload = searchApplicationsPayload(authOM2M,to_app,'123456')
# Send message and wait for response
sendAndWaitMQTT(client,topicReq,payload,waitTime,retryWaitTime,maxWaitTime)
# Check if this device is in the list
if json.loads(xware_globals.messageString)['m2m:rsp']['m2m:pc']['m2m:uril']:
    apps = lastUrlItem(json.loads(xware_globals.messageString)['m2m:rsp']['m2m:pc']['m2m:uril']['m2m:uril'])
else: apps = []
# Otherwise, create app
if not(deviceName in apps):
    payload = createApplicationPayload(authOM2M,to_app,'123456',F,t,T,valueConversion,deviceTag,sensorTag,deviceName)
    sendAndWaitMQTT(client,topicReq,payload,waitTime,retryWaitTime,maxWaitTime)


# ===========
# Check if OM2M container exists via MQTT
payload = searchContainersPayload(authOM2M,to_cont,'123456')
# Send message and wait for response
sendAndWaitMQTT(client,topicReq,payload,waitTime,retryWaitTime,maxWaitTime)
# Check if this container is in the list
if json.loads(xware_globals.messageString)['m2m:rsp']['m2m:pc']['m2m:uril']:
    containers = lastUrlItem(json.loads(xware_globals.messageString)['m2m:rsp']['m2m:pc']['m2m:uril']['m2m:uril'])
else: containers = []
# Otherwise, create container
if not(containerName in containers):
    payload = createContainerPayload(authOM2M,to_cont,'123456',containerName)
    sendAndWaitMQTT(client,topicReq,payload,waitTime,retryWaitTime,maxWaitTime)


# ===========
# Check if OM2M container (events) exists via MQTT
# Otherwise, create container
if not(eventsContName in containers):
    payload = createContainerPayload(authOM2M,to_cont,'123456',eventsContName)
    sendAndWaitMQTT(client,topicReq,payload,waitTime,retryWaitTime,maxWaitTime)


#====================
# OPEN EMPTY BUFFER
deviceBuffer = '' # Store as str
currentBufferSize = 0
totalSamples = 0


# ======================
# Set clock for first period
startTimePeriodT = time.time()
currentTimePeriodT = startTimePeriodT
nextTimePeriodT = currentTimePeriodT + T

while True:

    print('Begin cycle!')

    # ======================
    # Start a new sampling cycle (t)
    # Send START as MQTT+OM2M message, and wait for response
    messageIndex += 1
    payload = createMessagePayload(authOM2M,to_events,'123456','START\n'+deviceName+'\n'+str(messageIndex))
    sendAndWaitMQTT(client,topicReq,payload,waitTime,retryWaitTime,maxWaitTime)

    # ======================
    # WAIT FOR  RESPONSE
    ready = 0
    while not(ready):
        time.sleep(waitTime)

        # Search for all messages
        payload = searchMessagesPayload(authOM2M,to_events,'123456')
        sendAndWaitMQTT(client,topicReq,payload,waitTime,retryWaitTime,maxWaitTime)
        obj = json.loads(xware_globals.messageString)

        # If there are messages
        if obj['m2m:rsp']['m2m:pc']:
            if obj['m2m:rsp']['m2m:pc']['m2m:uril']:
                messageList = lastUrlItem(obj['m2m:rsp']['m2m:pc']['m2m:uril']['m2m:uril'])

                # Read each message
                for messageName in messageList:
                    payload = readMessagePayload(authOM2M,to_events+'/'+messageName,'123456')
                    sendAndWaitMQTT(client,topicReq,payload,waitTime,retryWaitTime,maxWaitTime)
                    obj = json.loads(xware_globals.messageString)
                    try:
                        messageText = obj["m2m:rsp"]["m2m:pc"]["m2m:cin"]["con"][1:-1]
                    except:
                        messageText = '     '

                    # Check if this is the message we need
                    if messageText[:5] == 'TIMER':

                        # Indicate readiness and delete received message
                        ready = 1
                        payload = deleteMessagePayload(authOM2M,to_events+'/'+messageName,'123456')
                        sendAndWaitMQTT(client,topicReq,payload,waitTime,retryWaitTime,maxWaitTime)


    # ======================
    # Set clock for first sample
    startTime = time.monotonic()
    currentTime = startTime
    nextTime = currentTime + deltaTime

    # ======================
    # 1/F loop: request sensor data

    samplesInSampling = int(t*F)
    while currentBufferSize < samplesInSampling:

        # ======================
        # Request and get sensor value
        value = getValueFromSensor()

        # Append value to device buffer
        deviceBuffer += (str(value)+'\n')

        # Update read samples
        currentBufferSize += 1

        # Stop the code until enough time has passed
        while time.monotonic() < nextTime:
            time.sleep(sampleWaitTime)

        # Update current and next time
        currentTime = nextTime
        nextTime += deltaTime

    # Debug
    print('Done reading data! Sending buffer...')

    # Send device buffer as MQTT+OM2M message
    payload = createMessagePayload(authOM2M,to_data,'123456',deviceBuffer)
    client.publish(topicReq, payload)

    # Clear buffer
    deviceBuffer = ''
    currentBufferSize = 0

    # Wait for next cycle
    print('Done sending data! Waiting for next period...')
    while time.time() < nextTimePeriodT:
        time.sleep(sampleWaitTime)

    # Update clock time for next sampling
    currentTimePeriodT = nextTimePeriodT
    nextTimePeriodT += T
