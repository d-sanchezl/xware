# Library of XRepo-related functions (data upload and download)

# Import necessary packages
import requests
import re
import json
import time
import datetime
import os

# ===========================================
# Return XRepo authentication token as a string

# username = 'user'
# password = 'user'

def xRepoAuth(username='user', password='user', url='http://xrepo.westus2.cloudapp.azure.com:8080/api/authenticate'):
    # Payload
    payload = {'username': username, 'password': password, 'rememberMe': 0}
    # Request auth token
    try:
        response = requests.post(url=url, json=payload)
    except:
        exit()
    if(response.status_code==200):
        return str(response.content, 'utf-8')[13:-2]
    else:
        exit()

# ===========================================
# Upload XRepo csv

# samplingID = Sampling ID, assigned previously and manually in XRepo
# csvFilename = string containing the filename, '.csv' included
# csvLocation = string containing the absolute folder location of the csv
#   (not including the filename)
# token = authentication token string, acquired from e.g. xRepoAuth()

# As of the time of writing, the proper input url is:
# url = "http://xrepo.westus2.cloudapp.azure.com:8080/api/samples-files-2"

def xRepoSend(samplingId, csvFilename, csvLocation, token, url='http://xrepo.westus2.cloudapp.azure.com:8080/api/samples-files-2'):
    multipartPayload = {"samplingId":('', samplingId), "file":(csvFilename, open(csvLocation+"/"+csvFilename, "rb"))}
    head = {'Authorization': 'Bearer ' + token}
    try:
        response = requests.post(url=url, files=multipartPayload, headers=head)
    except:
        exit()
    if(response.status_code==200):
        #obj = json.loads(response.text)
        return response.status_code #obj['batchTaskId']
    else:
        exit()

# ===========================================
# Get Experiment from Sampling

def xRepoGetExperiment(samplingId, token, url = 'http://xrepo.westus2.cloudapp.azure.com:8080/api/samplings'):
    urlId = url + '/' + samplingId
    head = {'Authorization': 'Bearer ' + token}
    try:
        response = requests.get(url=urlId, headers=head)
    except:
        exit()
    try:
        obj = json.loads(response.text)
    except:
        exit()
    return obj['experimentId']

# ===========================================
# Get System from Experiment

def xRepoGetSystem(experimentId, token, url = 'http://xrepo.westus2.cloudapp.azure.com:8080/api/experiments'):
    urlId = url + '/' + experimentId
    head = {'Authorization': 'Bearer ' + token}
    try:
        response = requests.get(url=urlId, headers=head)
    except:
        exit()
    try:
        obj = json.loads(response.text)
    except:
        exit()
    return obj['systemId']

# ===========================================
# Create an XRepo search, given a Sampling

# sT and eT are integer lists that describe the start time and end time respectively.
# They are in the form [year, month, day, hour, minute, second]

# XRepo's behavior with decimal seconds is undocumented, so it is recommended that
# you enter a start one second before the intended search, and the end one second after
# the intended search

def xRepoCreateSearch(samplingId,sT,eT,offsetGMT,token,url = 'http://xrepo.westus2.cloudapp.azure.com:8080/api/samples/data'):
    # Get Experiment ID
    experimentId = xRepoGetExperiment(samplingId, token)
    print('Experiment ID: ' + experimentId)
    # Get System ID
    systemId = xRepoGetSystem(experimentId, token)
    print('System ID: ' + systemId)
    # Start Time
    sTstr = ' '.join(str(i) for i in sT)  # Convert to string
    sTdate = datetime.datetime.strptime(sTstr, '%Y %m %d %H %M %S') # Convert to time structure
    sTdateG = sTdate - datetime.timedelta(hours=offsetGMT) # Convert to GMT
    sTstrG = sTdateG.strftime('%Y-%m-%dT%H:%M:%S') # Convert back to string
    # End Time
    eTstr = ' '.join(str(i) for i in eT)  # Convert to string
    eTdate = datetime.datetime.strptime(eTstr, '%Y %m %d %H %M %S') # Convert to time structure
    eTdateG = eTdate - datetime.timedelta(hours=offsetGMT) # Convert to GMT
    eTstrG = eTdateG.strftime('%Y-%m-%dT%H:%M:%S') # Convert back to string
    # Payload
    payload = {"targetSystemId": [systemId],
    "fromDateTime": sTstrG,
    "toDateTime": eTstrG}
    # Send
    head = {'Authorization': 'Bearer ' + token}
    try:
        response = requests.post(url=url, json=payload, headers=head)
    except:
        exit()
    if(response.status_code==200):
        obj = json.loads(response.text)
        return obj['batchTaskId']
    else:
        exit()

# ===========================================
# Check task status

# The task is complete once the function returns the 'COMPLETE' string
def xRepoGetTask(searchId, token, url = 'http://xrepo.westus2.cloudapp.azure.com:8080/api/batch-tasks'):
    urlId = url + '/' + searchId
    head = {'Authorization': 'Bearer ' + token}
    try:
        response = requests.get(url=urlId, headers=head)
    except:
        exit()
    try:
        obj = json.loads(response.text)
    except:
        exit()
    return obj['state']

# ===========================================
# Wait for batch task to complete

def xRepoWaitTask(taskId, token, uploadWaitTime, maxWaitTime):
    print('Task ID: ' + taskId)
    clock = time.time()
    status = ''
    while not(status == 'COMPLETED'):
        # Check for time threshold
        if time.time()-clock > maxWaitTime:
            print('Maximum wait time reached!')
            print('Please manually check the status of Batch Task ' + taskId)
            exit()
        if status == 'ERROR':
            print('An XRepo error occured')
            exit()
        # Wait
        time.sleep(uploadWaitTime)
        # Check new state
        status = xRepoGetTask(taskId, token)
        print(status)
    print('Task complete! Time required: ' + str(time.time()-clock))
    return 1

# ===========================================
# Get file list from search results

def xRepoSearchFiles(searchId, token, url = 'http://xrepo.westus2.cloudapp.azure.com:8080/api/batch-tasks/search-reports/file'):
    urlId = url + '/' + searchId
    head = {'Authorization': 'Bearer ' + token}
    try:
        response = requests.get(url=urlId, headers=head)
    except:
        exit()
    return response.text

# ===========================================
# Get file from url

def xRepoGetFile(fileId, token, url = 'http://xrepo.westus2.cloudapp.azure.com:8080'):
    urlId = url + '/' + fileId
    head = {'Authorization': 'Bearer ' + token}
    try:
        response = requests.get(url=urlId, headers=head)
    except:
        exit()
    return response.text

# ===========================================
# Download files from a search

def xRepoDownloadSearch(searchId, token, subfolder='xrepo_output'):
    fileList = xRepoSearchFiles(searchId, token)
    # Check if fileList is valid
    try:
        if not(fileList[:9]=='Processed'):
            print('Returned file list is anomalous')
            exit()
    except:
        print('Returned file list is anomalous')
        exit()
    print('Starting download...')
    # Create directory for files
    if not(os.path.isdir(subfolder)):
        os.mkdir(subfolder)
    # Read line by line
    fileInd = 0
    for line in fileList.splitlines()[1:]:
        # Extract file url
        ind = line.find(',')
        fileId = line[ind+3:]
        # Get file contents
        data = xRepoGetFile(fileId, token)
        # If not empty, save file
        if data:
            fileInd += 1
            file = open(subfolder+'/xrepo_output_'+str(fileInd)+'.csv', 'w')
            file.write(data)
            file.close()
    # Print result
    print('Non-empty files downloaded!')

# ===========================================
# Get list of latest uploaded files

# Size = number of search results to get, e.g. 20
# page = page of rsults to query; defaults to 0, the first
#   page with the newest results

# Results are given as a csv list, where every line contains:
# id,filename,state,createDate

def xRepoListUploadTasks(size,token,page=0,url = 'http://xrepo.westus2.cloudapp.azure.com:8080/api/batch-tasks/upload-files'):
    head = {'Authorization': 'Bearer ' + token}
    urlList = url + '?page='+str(page)+'&size='+str(size)+'&sort=createDate,desc'
    # download results
    try:
        response = requests.get(url=urlList, headers=head)
    except:
        exit()
    try:
        obj = json.loads(response.text)
    except:
        exit()
    csvString = ''
    # Extract data from the results
    for item in obj:
        filename = re.search('\w*.(c|C)(s|S)(v|S)$', item['description']).group(0)
        line = item['id'] + ',' + filename + ',' + item['state'] + ',' + item['createDate'] + '\n'
        csvString += line
    # Result
    return csvString
