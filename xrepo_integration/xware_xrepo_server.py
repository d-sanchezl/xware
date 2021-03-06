# OM2M to XRepo

# Import necessary packages
import paho.mqtt.client as mqtt # MQTT
import os # To create and manage directories
import time # To time sending intervals

# Import xware libraries
from xware_lib_functions import *
from xware_lib_om2m import *
from xware_lib_xrepo import *

# =================================
# PARAMETERS

# Simulated acquisition folder
csvLocation = '/home/pi/Documents/gateway_buffer'

# Log files location
logLocation = '/home/pi/Documents/python'

# OM2M parameters
serverCSE = 'in-cse'
serverName = 'in-name'
containerName = 'sampling'
eventsContName = 'events'
ipOM2M = '127.0.0.1:8080'
authOM2M = 'admin:admin'

# Wait time between cycles (can be 0)
waitTime = 0.05

# CSV storage parameters
timePrecision = 6
valuePrecision = 5

# XRepo parameters (changelog: added)
sendToXRepo = 1 # set to 0 to disable XRepo uploading
samplingId = "5f79e479e750ea746ad86fc9"
usernameXRepo = 'user'
passwordXRepo = 'user'


# ======================
# SET UP FLAGS AND VARIABLES

# Variables
startTimers = {}
csvNumber = 0

#====================
# Check for directory existance and start LOG
if not(os.path.isdir(csvLocation)):
    os.mkdir(csvLocation)

if not(os.path.isdir(logLocation)):
    os.mkdir(logLocation)
fullLogLoc = logLocation + '/' + 'log.txt'
fullTimerLoc = logLocation + '/' + 'timer.txt'
printAndLog('deviceName\tFlight\tCSV\tXRepo ',fullTimerLoc) #(changelog: XRepo)

printAndLog('gateway is active',fullLogLoc)

#====================
# BEGIN CYCLING

# Start buffers
valueBuffer = []

while 1:

    # Get URL list of devices
    applicationListUrl = listApplicationsREST(authOM2M,ipOM2M,serverCSE,serverName)
    # Get device names from url's
    devicesList = lastUrlItem(applicationListUrl)
    # Number of devices
    numOfDevices = len(devicesList)

    # Create dictionary entry for devices if necessary
    for deviceName in devicesList:
        if not(deviceName in startTimers):
            startTimers[deviceName] = {'currentIndex':0}

    # Cycle through devices in list
    for deviceName in devicesList:

        #====================
        # CHECK FOR RECEIVED EVENTS

        # Get messages list
        messageListUrl = listMessagesREST(authOM2M,ipOM2M,serverCSE,serverName,deviceName,eventsContName)
        messageList = lastUrlItem(messageListUrl)

        # Check if there are messages
        for messageName in messageList:
            # Process each message
            messageText = getMessageREST(authOM2M,ipOM2M,serverCSE,serverName,deviceName,eventsContName,messageName)
            if messageText[:5] == 'START':
                # Delete message from OM2M
                deleteMessageREST(authOM2M,ipOM2M,serverCSE,serverName,deviceName,eventsContName,messageName)
                # Extract message contents
                oldMessage = messageText.splitlines()
                index = int(oldMessage[2])
                # Store in dictio
                startTimers[deviceName][index] = time.time()
                # Talkback to device
                newMessage = 'TIMERBEGIN\n'+deviceName+'\n'+str(index)
                createMessageREST(authOM2M,ipOM2M,serverCSE,serverName,deviceName,eventsContName,newMessage)

        #====================
        # CHECK FOR RECEIVED VALUES

        # Get messages list
        messageListUrl = listMessagesREST(authOM2M,ipOM2M,serverCSE,serverName,deviceName,containerName)
        messageList = lastUrlItem(messageListUrl)

        # Check if there are pending messages
        if messageList:

            # Grab the first message in the list!
            messageName = messageList[0]

            # Download message contents from OM2M
            valueBuffer = getMessageREST(authOM2M,ipOM2M,serverCSE,serverName,deviceName,containerName,messageName).splitlines()

            # Read metadata of this device
            labels = readApplicationLabelsREST(authOM2M,ipOM2M,serverCSE,serverName,deviceName)
            F = float(labels['Frequency[Hz]'])
            valueConversion = float(labels['ValueConversion'])
            deviceTag = labels['Device']
            sensorTag = labels['Sensor']

            # ===== Get start time =====
            # Update current index for this device
            startTimers[deviceName]['currentIndex'] += 1
            currentIndex = startTimers[deviceName]['currentIndex']
            startTime = startTimers[deviceName][currentIndex]

            # Flight time
            timerFlight = time.time() - startTime

            # Create new CSV
            fileName = 'sample_' + str(csvNumber) + '_' + deviceName + '.csv'
            file = open(csvLocation + '/' + fileName, 'w')
            file.write('ID,,,\n')

            # Time delta
            deltaTime = 1/F
            currentTime = 0

            # Create each CSV line
            for valueStrRaw in valueBuffer:
                timeUnix = preciseUnixTime(startTime,0,currentTime,timePrecision,string=1)
                valueStr = float(valueStrRaw)*valueConversion
                csvLine = csvFormatLine(timeUnix,deviceTag,sensorTag,valueStr,valuePrecision)
                file.write(csvLine)
                currentTime += deltaTime

            # End CSV
            file.close()
            csvNumber += 1
            printAndLog(deviceName + ' CSV file created',fullLogLoc)

            # CSVTime
            timerCSV = time.time() - startTime

            # Upload to XRepo and delete from local storage (changelog: added)
            if sendToXRepo:

                # XREPO AUTH
                token = xRepoAuth(usernameXRepo, passwordXRepo)
                if not(token):
                    printAndLog('XRepo authentication failed, terminating gateway',fullLogLoc)
                    exit()
                printAndLog(deviceName + ' XR authentication successful',fullLogLoc)

                # XREPO SEND
                code = xRepoSend(samplingId, fileName, csvLocation, token)
                if not(code):
                    printAndLog('XRepo upload failed, terminating gateway',fullLogLoc)
                    exit()
                else:
                    os.remove(csvLocation+"/"+fileName)
                printAndLog(deviceName + ' XR file uploaded',fullLogLoc)

                # Append 2:XRTime
                timerXR = time.time() - startTime

            else:
                timerXR = 0

            # Print timers (changelog: added xrepo timer)
            timerString = deviceName + '\t' + str(timerFlight) + \
                          '\t' + str(timerCSV) + '\t' + str(timerXR)
            printAndLog(timerString,fullTimerLoc)

            # CLEAR gateway local buffer
            valueBuffer = []

            # CLEAR gateway OM2M buffer
            deleteMessageREST(authOM2M,ipOM2M,serverCSE,serverName,deviceName,containerName,messageName)

    # Let the program breathe!
    time.sleep(waitTime)
