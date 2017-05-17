#!/usr/bin/python
__author__ = 'shastri@umass.edu'

import argparse
from datetime import datetime
import os 
import sys
import operator
import re
from decimal import *


gBeginTime = 1482192000 + (0 * 86400) # Dec-20 + additional days
#gBeginTime = 1488326400 + (0 * 86400) # Mar 1, 2017 + additional days

# { 'instType' : [CPU, nECU, memory, weight] }
instECUMemoryDict = dict()

# Dictionary of on-demand price dictionaries
# Each region's dictionary will have the following format:
# { 'instType' : [price, nECU] }
onDemandDict = {    
    'us-west-1'         : dict(),
    'us-west-2'         : dict(),
    'us-east-1'         : dict(),
    'us-east-2'         : dict(),
    'eu-west-1'         : dict(),
    'eu-west-2'         : dict(),
    'eu-central-1'      : dict(),
    'ca-central-1'      : dict(),
    'ap-south-1'        : dict(),
    'ap-southeast-1'    : dict(),
    'ap-southeast-2'    : dict(),
    'ap-northeast-1'    : dict(),
    'ap-northeast-2'    : dict(),
    'sa-east-1'         : dict()
}

def nth_root(val, n):
    ret = val**(1./n)
    return ret

def buildOndemandDict(ondemandDir):
    global onDemandDict
    global instECUMemoryDict

    instInfo = open("ec2-servers.config").read().splitlines()
    for line in instInfo:
        instType, cpu, ecu, memory = line.split('\t')
        weight = nth_root(float(ecu) * float(memory), 2)
        instECUMemoryDict[instType] = [float(cpu), float(ecu), float(memory), Decimal(weight)]

    for priceFile in os.listdir(ondemandDir):
        region = re.sub('.data', '', priceFile)
        priceFile = ondemandDir + priceFile
        if os.path.isfile(priceFile):
            regionDict = onDemandDict[region] 
            with open(priceFile) as inFp:
                for line in inFp:
                    line = re.sub(' +', ' ', line)
                    instType, ecu, price = line.split(' ')
                    price = price.strip('$')
                    regionDict[instType] = [Decimal(price)*Decimal(100.0), float(ecu)]
    
    return
# END buildOndemandDict


def getEpochTime(spotTime):
    pattern = '%Y-%m-%dT%H:%M:%S'
    utcTime = datetime.strptime(spotTime, pattern)
    epochTime = int((utcTime - datetime(1970, 1, 1)).total_seconds())
    return epochTime
# [END getEpochTime]


# Use timestamp as key from the priceTrace lines.
def getKey(priceUpdateLine):
    v1, price, time, inst, v5, v6 = priceUpdateLine.split('\t')
    return getEpochTime(time)
# [END getKey]


def getOndemandInstName(spotMarket):
    # Extract on-demand instance name
    inst = re.sub('\.us-west-1[abcde]', '', spotMarket)
    inst = re.sub('\.us-west-2[abcde]', '', inst)
    inst = re.sub('\.us-east-1[abcde]', '', inst)
    inst = re.sub('\.us-east-2[abcde]', '', inst)
    inst = re.sub('\.eu-west-1[abcde]', '', inst)
    inst = re.sub('\.eu-west-2[abcde]', '', inst)
    inst = re.sub('\.eu-central-1[abcde]', '', inst)
    inst = re.sub('\.ca-central-1[abcde]', '', inst)
    inst = re.sub('\.ap-south-1[abcde]', '', inst)
    inst = re.sub('\.ap-southeast-1[abcde]', '', inst)
    inst = re.sub('\.ap-southeast-2[abcde]', '', inst)
    inst = re.sub('\.ap-northeast-1[abcde]', '', inst)
    inst = re.sub('\.ap-northeast-2[abcde]', '', inst)
    inst = re.sub('\.sa-east-1[abcde]', '', inst)
    inst = re.sub('\.vpc', '', inst)
    return inst
# END getOndemandInstName
 

def getRegionName(spotMarket):
    regionList = [  'us-west-1', 'us-west-2', 'us-east-1', 'us-east-2',
                    'eu-west-1','eu-west-2', 'eu-central-1',
                    'ap-south-1', 'ap-southeast-1', 'ap-southeast-2', 
                    'ap-northeast-1', 'ap-northeast-2', 
                    'ca-central-1', 'sa-east-1' ]  
    for region in regionList:
        if region in spotMarket:
            return region
    
    return 'invalid'
# END getRegionName


def processSpotUpdates(spotPriceDir, startTime = 0, endTime = 0):
    spotUpdateList = list()
    numMarkets = 0

    # Expecting startTime and endTime to be offset from gBeginTime.
    startTime += gBeginTime
    endTime += gBeginTime
    
    # Read all the EC2 price trace files into a dictionary of markets.
    # Each market would have a sorted list of price updates.
    for root, dirs, files in os.walk(spotPriceDir):
        for name in files:
            spotFile = os.path.join(root, name)
            vpcInfo = '.vpc.' if 'vpc' in name else '.'
            if os.path.isfile(spotFile) == False:
                continue

            spotUpdates = open(spotFile).read().splitlines()
            sortedUpdates = sorted(spotUpdates, key=getKey)

            v1, price, time, inst, v2, zone = sortedUpdates[0].split('\t')
            spotMarket = inst + vpcInfo + zone
            firstUpdate = True
            prevTime, prevPrice = 0, 0
            numMarkets += 1

            # Iterate through sorted updates and fillup the dict entry
            for update in sortedUpdates:
                v1, price, time, inst, v5, zone = update.split('\t')
                spotTime = getEpochTime(time)
                spotPrice = Decimal(price) * 100

                if spotTime <= startTime:
                    prevTime, prevPrice = startTime, spotPrice
                    continue
                
                elif spotTime > endTime:
                    # For those rare markets that don't have any entry in (startTime, endTime)
                    # time range, we retrogress the price all the way back to startTime.
                    if prevTime == 0 and prevPrice == 0:
                        spotUpdateList += [(startTime, spotPrice, spotMarket)]
                    break
                
                else:
                    if firstUpdate == True:
                        spotUpdateList += [(prevTime, prevPrice, spotMarket), (spotTime, spotPrice, spotMarket)]
                        firstUpdate = False
                    else:
                        spotUpdateList += [(spotTime, spotPrice, spotMarket)]
            
            print "Total: " + str(len(spotUpdateList)) + " entries after adding " + spotMarket

    # Now, sort the spotUpdateList based on spotTime
    spotUpdateList.sort(key=operator.itemgetter(0), reverse=False)
    return spotUpdateList, numMarkets 
# END processSpotUpdates


def computeIndex(spotUpdateList, numMarkets = 1, boundaryTime = 0, endTime = 0, timeInc = 3600):
    # Expecting startTime and endTime to be offset from gBeginTime.
    boundaryTime += gBeginTime
    endTime += gBeginTime
    
    # Dictionary of marketIndex at a granularity of <timeInc>
    marketIndexDict = dict()
 
    # Find starting price of all the markets
    curSpotRateDict = dict()
    for (spotTime, spotPrice, spotMarket) in spotUpdateList:
        
        if spotTime > boundaryTime:
            runningTotal = Decimal(0.0)
            numMarkets = 0
            minVal = Decimal(1000000.0)
            maxVal = Decimal(0.0)
            
            for curMarket, curRate in curSpotRateDict.items():
                runningTotal += curRate
                numMarkets += 1
                if minVal > curRate:
                    minVal = curRate
                if maxVal < curRate:
                    maxVal = curRate
            
            dictKey, dictVal = int(boundaryTime/timeInc), Decimal(runningTotal/numMarkets)
            marketIndexDict[dictKey] = (dictVal, minVal, maxVal)
        
            while spotTime > boundaryTime:
                boundaryTime += timeInc

        inst = getOndemandInstName(spotMarket)
        region = getRegionName(spotMarket)
    
        # Exclude all the markets that have no availability (i.e. price is 10x)
        if spotPrice >= 10 * onDemandDict[region][inst][0]:
            if spotMarket in curSpotRateDict:
                del curSpotRateDict[spotMarket]
        else:
            curSpotRateDict[spotMarket] = spotPrice / instECUMemoryDict[inst][3]
    
    return marketIndexDict
#END computeIndex

def writeResult(indexDict, filename, timeInc):
    # Flush the market index data into the output file
    outputStringAvg = ""
    resultFileAvg = filename + "avg"
    outputStringMin = ""
    resultFileMin = filename + "min"
    outputStringMax = ""
    resultFileMax = filename + "max"

    print "Time Inc: " + str(timeInc)

    for dictKey, (dictValAvg, dictValMin, dictValMax) in sorted(indexDict.items()):
        outputStringAvg += str(dictKey*timeInc) + "\t" + str(dictValAvg) + "\n" 
        outputStringMin += str(dictKey*timeInc) + "\t" + str(dictValMin) + "\n"
        outputStringMax += str(dictKey*timeInc) + "\t" + str(dictValMax) + "\n"    
    
    with open(resultFileAvg, 'w') as outFp:
        outFp.write(outputStringAvg)
   
    with open(resultFileMin, 'w') as outFp:
        outFp.write(outputStringMin)

    with open(resultFileMax, 'w') as outFp:
        outFp.write(outputStringMax)
  
    return
# END writeResult


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("ondemandDir", help="Path to on-demand directory")
    parser.add_argument("spotPriceDir", help="Path to spot price directory")
    parser.add_argument("resultDir", help="Path to result directory")
    args = parser.parse_args()
    buildOndemandDict(args.ondemandDir)

    # Time offsets in seconds
    startTime, endTime, timeInc = 0, (3600 * 24 * 60), 3600
    spotUpdateList, numMarkets = processSpotUpdates(args.spotPriceDir, startTime, endTime)
   
    print "\nStarting index calculations\n" 
    marketIndexDict = computeIndex(spotUpdateList, numMarkets, startTime, endTime, timeInc)
    writeResult(marketIndexDict, args.resultDir + "/index.", timeInc)

