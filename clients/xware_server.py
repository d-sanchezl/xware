# OM2M to local storage (csv files)
# See Github repo (github.com/d-sanchezl/xware) for license details

# Make sure you review the "USER PARAMETERS"
# sections before executing this code.


# Import necessary packages
import paho.mqtt.client as mqtt # MQTT
import os # To create and manage directories
import time # To time sending intervals

# Import xware libraries
from xware_lib_functions import *
from xware_lib_om2m import *



# ==================================================================
# USER PARAMETERS:
# Change these to your liking

# Acquisition folder:
# The resulting CSV files will be stored here
csvLocation = 'C:/Users/User/XWare/csv_files'

# Log files location:
# Log files (useful in case of a crash) will be stored here
logLocation = 'C:/Users/User/XWare/logs'

# CSV storage parameters
# Define the decimal places that time and values will have
timePrecision = 6
valuePrecision = 5


# =================================
# ADVANCED PARAMETERS:
# Do not change these unless you know what you are doing

# OM2M parameters
serverCSE = 'in-cse'
serverName = 'in-name'
containerName = 'sampling'
eventsContName = 'events'
ipOM2M = '127.0.0.1:8080'
authOM2M = 'admin:admin'

# Wait time between cycles (can be 0)
waitTime = 0.05



# ==================================================================
# XWARE CODE

# This is the XWare code.
# You should not have to change anything beyond this point.

# ======================
# SET UP FLAGS AND VARIABLES

# Variables
startTimers = {}

#====================
# Check for directory existance and start LOG
if not(os.path.isdir(csvLocation)):
    os.mkdir(csvLocation)

if not(os.path.isdir(logLocation)):
    os.mkdir(logLocation)
fullLogLoc = logLocation + '/' + 'log.txt'
fullTimerLoc = logLocation + '/' + 'timer.txt'
printAndLog('deviceName\tFlight\tCSV ',fullTimerLoc)

printAndLog('gateway is active',fullLogLoc)

#====================
# BEGIN CYCLING

# Indicate first cycle (to delete old apps)
starting = 1

# Start buffers
valueBuffer = []

while 1:

    # Get URL list of devices
    applicationListUrl = listApplicationsREST(authOM2M,ipOM2M,serverCSE,serverName)
    # Get device names from url's
    devicesList = lastUrlItem(applicationListUrl)
    # Number of devices
    numOfDevices = len(devicesList)

    # If starting, prompt to delete old apps
    if starting:
        if numOfDevices > 0:
            input('Press Return to delete old OM2M data and continue.')
            for deviceName in devicesList:
                deleteApplicationREST(authOM2M,ipOM2M,serverCSE,serverName,deviceName)
            print('Old data removed! You may start XWare (gateway) in your device(s).')
            print('')
        devicesList = []
        starting = 0

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
            print(startTime)

            # Flight time
            timerFlight = time.time() - startTime

            # Set time delta
            deltaTime = 1/F

            # Start time counter
            currentTime = 0

            # File name for new CSV
            dateStr = time.strftime('%Y%m%dT%H%M%S',time.localtime(startTime))
            decimalsStr = str(int((startTime-int(startTime))*(10**2)))
            fileName = deviceName + '_' + dateStr + decimalsStr + '.csv'

            # Create new CSV
            file = open(csvLocation + '/' + fileName, 'w')
            file.write('ID,,,\n')

            # Separte sensor tags (if there are multiple)
            sensorTagList = sensorTag.split(',')

            # ==============
            # Create each CSV line
            for valueStrRaw in valueBuffer:
                # Create time for this measurement
                timeUnix = preciseUnixTime(startTime,0,currentTime,timePrecision,string=1)
                # Separate values (if there are multiple)
                valueStringList = valueStrRaw.split(',')
                # Convert values to numbers, considering the conversion factor
                valueListConverted = []
                for singleValue in valueStringList:
                    valueListConverted.append(float(singleValue)*valueConversion)
                # Check that the length of the sensor tag list and sensor values is the same
                if len(sensorTagList) != len(valueListConverted):
                    print("Error: There are " + str(len(sensorTagList)) + "sensor tags but only " + len(valueListConverted) + "values.")
                # Cycle through the different tags and values, and save to csv
                for singleTag, singleValue in zip(sensorTagList, valueListConverted):
                    # Join everything into one text
                    csvLine = csvFormatLine(timeUnix,deviceTag,singleTag,singleValue,valuePrecision)
                    # Write text to file
                    file.write(csvLine)
                # Update time
                currentTime += deltaTime

            # End CSV
            file.close()
            printAndLog(deviceName + ' CSV file created',fullLogLoc)

            # CSVTime
            timerCSV = time.time() - startTime

            # Print timers
            timerString = deviceName + '\t' + str(timerFlight) + \
                          '\t' + str(timerCSV)
            printAndLog(timerString,fullTimerLoc)

            # CLEAR gateway local buffer
            valueBuffer = []

            # CLEAR gateway OM2M buffer
            deleteMessageREST(authOM2M,ipOM2M,serverCSE,serverName,deviceName,containerName,messageName)

    # Let the program breathe!
    time.sleep(waitTime)
