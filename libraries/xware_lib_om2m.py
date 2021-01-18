# Useful functions for OM2M and MQTT in XWare

# Functions adapted from Jairo Seros' work:
# https://github.com/SELF-Software-Evolution-Lab/Adaptive-Architecture-for-Transient-IoT-Systems

# Import necessary packages
import requests
import re
import json

# ===========================================
# Create a primitive content MQTT payload for OM2M

# Refer to https://wiki.eclipse.org/OM2M/one/MQTT_Binding
# for a description of the inputs

# fr = Authentication, in user:password form
# to = Target
# op = Operation to perform
# rqi = Request ID
# pc = Request contents
# ty = Object type

def primitiveContentPayload(fr,to,op,rqi,pc,ty):
    payload = '''{
"m2m:rqp": {
"m2m:fr" : "'''+fr+'''",
"m2m:to" : "'''+to+'''",
"m2m:op" : "'''+op+'''",
"m2m:rqi": "'''+rqi+'''",
"m2m:pc" : '''+pc+''',
"m2m:ty" : "'''+ty+'''"}}'''
    return payload


# ===========================================
# Create an MQTT payload, specifically to create an application

# F,t,T: Sensor frequency, sampling time, period
# sensor, var: XRepo tags
# appName: OM2M name for the application

def createApplicationPayload(auth,to,rqi,F,t,T,valueConversion,deviceTag,sensorTag,appName):
    op = '1' # Operation: Create
    ty = '2' # Type: Application
    pc = '''{
"m2m:ae": {
"api": "app-sensor",
"rr": "false",
"lbl": ["Frequency[Hz]/'''+str(F)+'''",
"SampleTime[s]/'''+str(t)+'''",
"Period[s]/'''+str(T)+'''",
"ValueConversion/'''+str(valueConversion)+'''",
"Device/'''+deviceTag+'''",
"Sensor/'''+sensorTag+'''"],
"rn": "'''+appName+'''"}}''' # Application metadata
    return primitiveContentPayload(auth,to,op,rqi,pc,ty)


# ===========================================
# Create an MQTT payload, specifically to create a container
# containerName: OM2M name for the container

def createContainerPayload(auth,to,rqi,containerName):
    op = '1' # Operation: Create
    ty = '3' # Type: Container
    pc = '''{"m2m:cnt": {"rn": "'''+containerName+'''"}}'''
    return primitiveContentPayload(auth,to,op,rqi,pc,ty)


# ===========================================
# Create an MQTT payload, specifically to create a message
# message: message contents

def createMessagePayload(auth,to,rqi,message):
    op = '1' # Operation: Create
    ty = '4' # Type: Message
    pc = '''{"m2m:cin": {"cnf": "message", "con": "'''+message+'''"}}'''
    return primitiveContentPayload(auth,to,op,rqi,pc,ty)


# ===========================================
# Create an MQTT payload, specifically to read a message

def readMessagePayload(auth,to,rqi):
    op = '2' # Operation: Retrieve
    ty = '4' # Type: Message
    pc = '""'
    return primitiveContentPayload(auth,to,op,rqi,pc,ty)


# ===========================================
# Create an MQTT payload, specifically to delete a message
# message: message contents

def deleteMessagePayload(auth,to,rqi):
    op = '4' # Operation: Delete
    ty = '4' # Type: Message
    pc = '""'
    return primitiveContentPayload(auth,to,op,rqi,pc,ty)


# ===========================================
# Create a 'filterCriteria' MQTT payload for OM2M

# fr = Authentication, in user:password form
# to = Target
# rqi = Request ID
# ty = Object type

def filterCriteriaPayload(fr,to,rqi,ty):
    payload = '''{
"m2m:rqp": {
"m2m:fr" : "'''+fr+'''",
"m2m:to" : "'''+to+'''",
"m2m:op" : "2",
"m2m:rqi": "'''+rqi+'''",
"m2m:fc" : {"m2m:fu": "1", "m2m:ty":"'''+ty+'''"}}}'''
    return payload


# ===========================================
# Create an MQTT payload, specifically to find all applications

def searchApplicationsPayload(auth,to,rqi):
    ty = '2' # Type: Application
    return filterCriteriaPayload(auth,to,rqi,ty)


# ===========================================
# Create an MQTT payload, specifically to find all containers

def searchContainersPayload(auth,to,rqi):
    ty = '3' # Type: Application
    return filterCriteriaPayload(auth,to,rqi,ty)


# ===========================================
# Create an MQTT payload, specifically to find all messages

def searchMessagesPayload(auth,to,rqi):
    ty = '4' # Type: Message
    return filterCriteriaPayload(auth,to,rqi,ty)


# ===========================================
# Send a message via OM2M

def createMessageREST(auth="admin:admin",ip="127.0.0.1:8080",serverCSE="in-cse",serverName="in-name",appName="",containerName="",message=""):
    # Build and send POST
    header = {"X-M2M-Origin": auth, "Content-Type" : "application/json;ty=4"}
    url = "http://"+ip+"/~/"+serverCSE+"/"+serverName+"/"+appName+"/"+containerName
    payload = {"m2m:cin": {"cnf": "message", "con": '"'+message+'"'}}
    return requests.post(url, json=payload, headers=header)


# ===========================================
# List all OM2M applications

def listApplicationsREST(auth="admin:admin",ip="127.0.0.1:8080",serverCSE="in-cse",serverName="in-name"):
    # Build and send GET
    header = {"X-M2M-Origin": auth, "Accept": "application/json"}
    url = "http://"+ip+"/~/"+serverCSE+"/"+serverName+"?fu=1&ty=2"
    response = requests.get(url, headers=header)
    # If successful, find individual message names
    if response.status_code == 200:
        # Get actual message contents
        obj = json.loads(response.text)
        return obj['m2m:uril']
    else:
        return []


# ===========================================
# List all OM2M messages in a container

def listMessagesREST(auth="admin:admin",ip="127.0.0.1:8080",serverCSE="in-cse",serverName="in-name",appName="",containerName=""):
    # Build and send GET
    header = {"X-M2M-Origin": auth, "Accept": "application/json"}
    url = "http://"+ip+"/~/"+serverCSE+"/"+serverName+"/"+appName+"/"+containerName+"?fu=1&ty=4"
    response = requests.get(url, headers=header)
    # If successful, find individual message names
    if response.status_code == 200:
        # Get actual message contents
        obj = json.loads(response.text)
        return obj['m2m:uril']
    else:
        return []


# ===========================================
# Read the labels of a specific application
# The output is a dictionary of the labels and their values

def readApplicationLabels(auth="admin:admin",ip="127.0.0.1:8080",serverCSE="in-cse",serverName="in-name",appName=""):
    # Build and send GET
    header = {"X-M2M-Origin": auth, "Accept": "application/json"}
    url = "http://"+ip+"/~/"+serverCSE+"/"+serverName+"/"+appName
    response = requests.get(url, headers=header)
    # If successful, find app labels
    if response.status_code == 200:
        obj = json.loads(response.text)
        objList = obj['m2m:ae']['lbl']
        # Create dictionary of labels
        dictio = {}
        for label in objList:
            slashIndex = label.find('/')
            tagName = label[:slashIndex]
            tagValue = label[slashIndex+1:]
            dictio[tagName]=tagValue
        return dictio
    else:
        return {}


# ===========================================
# Read a specific message from OM2M

def getMessageREST(auth="admin:admin",ip="127.0.0.1:8080",serverCSE="in-cse",serverName="in-name",appName="",containerName="",messageName=""):
    # Build and send GET
    header = {"X-M2M-Origin": auth, "Accept": "application/json"}
    url = "http://"+ip+"/~/"+serverCSE+"/"+serverName+"/"+appName+"/"+containerName+"/"+messageName
    response = requests.get(url, headers=header)
    # If successful, find message contents
    if response.status_code == 200:
        obj = json.loads(response.text)
        return obj['m2m:cin']['con']
    else:
        return []


# ===========================================
# Delete a specific message from OM2M

def deleteMessageREST(auth="admin:admin",ip="127.0.0.1:8080",serverCSE="in-cse",serverName="in-name",appName="",containerName="",messageName=""):
    # Build and send GET
    header = {"X-M2M-Origin": auth, "Accept": "application/json"}
    url = "http://"+ip+"/~/"+serverCSE+"/"+serverName+"/"+appName+"/"+containerName+"/"+messageName
    response = requests.delete(url, headers=header)


# ===========================================
# Get the last URL items for a list of URL's

# urlList = single url string or list of url strings

def lastUrlItem(urlList):
    itemsList = []
    # Verify if the input is a single string and act accordingly
    if isinstance(urlList, str):
        urlList = [urlList]
    # For every item in the list
    for item in urlList:
        # Find the last slash
        index = None
        for m in re.finditer(r"/", item):
            index = m.start()
        # Add everything after the last slash
        if not(index is None):
            itemsList.append(item[index+1:])
        else:
            itemsList.append(item)
    return itemsList
