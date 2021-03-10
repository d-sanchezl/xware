# MQTT to OM2M gateway device client with simulated aquisition

# Import necessary packages
import paho.mqtt.client as mqtt # MQTT
import os # To create and manage directories
import time # To time sending intervals

# Import xware libraries
from xware_lib_functions import *
from xware_lib_om2m import *
import xware_globals

# =================================
# PARAMETERS

# Sampling Frequency
F = 20000
# Time length of a single sampling [s]
t = 2
# Time interval between samplings [s]
T = 10
# Voltage to value scaling
valueConversion = 1/0.00989

# Context tags
deviceTag = 'accel'
sensorTag = 'x_accel'

# MQTT topic name
deviceName = 'device0'
# MQTT address, change to target IP address
brokerAddress = '192.138.6.70' # Replace with the IP of the Server as a string

# Simulated acquisition folder
samplesLocation = "/home/pi/Documents/amest_txt_divided"

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

# =================================
# FUNCTION

# Read next TXT line
def getValueFromTxt():
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


# ======================
# SET UP FLAGS AND VARIABLES
# # globalStartSuccess = 0
# # startSuccess = 0
xware_globals.newMessage = None
xware_globals.messageString = ''
messageIndex = 0

# Simulated Acquisition
fileList = [(samplesLocation+"/"+i) for i in os.listdir(samplesLocation)]
currentFileIndex = 0
currentLineIndex = 0
lineList = []


# ======================
# SET UP 'SIMULATED' DATA AQUISITION

# Get names of all txt files in the folder
fileNames = os.listdir(samplesLocation)

# Read all file names in selected folder
fileLocs = [(samplesLocation+"/"+i) for i in fileNames]
totalFiles = len(fileLocs)
fileIndex = 0

# Simulation parameters
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

while 1:

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
        value = getValueFromTxt()

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
