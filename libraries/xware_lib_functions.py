# Miscellaneous functions used in XWare
# See Github repo (github.com/d-sanchezl/xware) for license details

# Import necessary packages
import os
import time
import math

# ===========================================
# Separate elements in a line into a list, given a separator

# line: string to be separated
# separator: character to use to divide the line
# max_iter: maximum instances of the separator

def separateStringFinder(line,separator='_',max_iter=1000):
    listed = []
    start = 0
    iterations = 0
    while 1:
        end = line[start:].find(separator)
        if end != -1:
            listed.append(line[start:start+end])
        else:
            listed.append(line[start:])
            return listed
        start += end+1
        iterations += 1
        if iterations >= max_iter:
            return ['Error: too many fields']


# ===========================================
# In a 'time\tvalue' string, extract the value

# The last character (usually a \n) is removed, too
# The output is NOT converted to float. It is still a str

def valueFromString(line):
    tabPos = line.find('\t')
    if tabPos == -1:
        tabPos = line.find(',')
    valueStr = line[tabPos+1:-1]
    return valueStr


# ===========================================
# Create a list or string of the curren UNIX time float with a given precision

# This function requires an 'anchor' to have been created: at a point in time, both a UNIX (POSIX) timestamp and an an arbitrary high precision timestamp (e.g. monotonic time) have to be created.

# Then, this function can create an UNIX-like timestamp from a new high precision timestamp. This allows for UNIX-like times to be created for high-frequency samples, where the regular UNIX precision is insufficient.

# (All inputs are floats/ints)

# startUnix: the starting anchor time in UNIX, e.g. obtained from time.time()
# startMono: the starting anchor time in high precision time, e.g. obtained from time.monotonic()
# currentMono: current timestamp in high precision time
# string: set to 1 to output a string; otherwise, output a list with the integer and decimal positions separated

def preciseUnixTime(startUnix,startMono,currentMono,precision=16,string=0):
    # Deal with excessive precision
    if precision > 16:
        precision = 16
    # Round down inputs
    intStartUnix = math.floor(float(startUnix))
    # Time that has passed
    passed = currentMono-startMono
    intPassed = math.floor(float(passed))
    # Integer of current time
    currentInt = intStartUnix + intPassed
    # Decimals of current time
    currentDecimal = (startUnix-float(intStartUnix)) + (passed-float(intPassed))
    intCurrentDecimal = math.floor(float(currentDecimal))
    # Correct for 'decimals' higher than 1
    currentInt += intCurrentDecimal
    currentDecimal -= intCurrentDecimal
    # Return string or list
    if string:
        return str(currentInt) + format(round(currentDecimal,precision), '.'+str(precision)+'f')[1:]
    else:
        return [currentInt, round(currentDecimal,precision)]

# ===========================================
# Convert a standard UNIX time number to a datetime string with decimals

# unixTime: float time in UNIX, e.g. obtained from time.time()

def unixToDateString(unixTime,form='%Y-%m-%dT%H:%M:%S',precision=7):
    timeUnixStr = format(unixTime, '.'+str(precision)+'f')
    decPos = timeUnixStr.find('.')
    if decPos == -1:
        unixInt = int(timeUnixStr)
    else:
        unixInt = int(timeUnixStr[0:decPos])
    timeStruct = time.localtime(unixInt)
    # Create datetime string
    return time.strftime(form,timeStruct) + timeUnixStr[decPos:]


# ===========================================
# Write a single line to a file
# Make sure to include a \n at the end if a new line is desired
def writeLineToFile(fullFileName,line):
    file = open(fullFileName, 'a')
    file.write(line)
    file.close()


# ===========================================
# Print to both the console output and to a specified log file

# A \n at the end is not necessary, this function adds one
# Add a \n to add an empty line

def printAndLog(printLine,fullFileLocation):
    print(printLine)
    writeLineToFile(fullFileLocation, \
        '[' + unixToDateString(time.time()) + ']\t' + printLine + '\n')


# ===========================================
# Create an CSV sample line

# All inputs are strings, except 'precision' which is a float

def csvFormatLine(timeUnixStr,deviceStr,sensorStr,valueStr,valuePrecision=16):
    # Extract unix time and convert
    decPos = timeUnixStr.find('.')
    if decPos == -1:
        unixInt = int(timeUnixStr)
    else:
        unixInt = int(timeUnixStr[0:decPos])
    timeStruct = time.localtime(unixInt)
    # Create datetime string
    newDateStr = time.strftime('%Y-%m-%dT%H:%M:%S',timeStruct) + timeUnixStr[decPos:]
    # Correct value
    newValueStr = format(round(float(valueStr),valuePrecision), '.'+str(valuePrecision)+'f')
    return newDateStr + ',' + deviceStr + ',' + sensorStr + ',' + newValueStr + '\n'
