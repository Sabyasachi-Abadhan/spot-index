#!/usr/bin/python
__author__ = 'shastri@umass.edu'

import boto3
from datetime import datetime
import time
from decimal import *

#------------------------------------------
# Globals initialized to default values
#------------------------------------------
spotIndex_region = ''

EC2ZoneDict = { 'us-west-1':['a', 'b', 'c'], 
                'us-west-2':['a', 'b', 'c'], 
                'us-east-1':['a', 'b', 'c', 'd', 'e'],
                'us-east-2':['a', 'b', 'c'],
                'eu-west-1':['a', 'b', 'c'], 
                'eu-west-2':['a', 'b'], 
                'eu-central-1':['a', 'b'],
                'ca-central-1':['a', 'b'],
                'ap-south-1':['a', 'b'], 
                'ap-southeast-1':['a', 'b'], 
                'ap-southeast-2':['a', 'b', 'c'],
                'sa-east-1':['a', 'b', 'c'], 
                'ap-northeast-1':['a', 'b', 'c'], 
                'ap-northeast-2':['a', 'c'] }

EC2InstList = ['t1.micro','t2.nano','t2.micro','t2.small','t2.medium','t2.large','m1.small','m1.medium','m1.large','m1.xlarge','m3.medium','m3.large','m3.xlarge','m3.2xlarge','m4.large','m4.xlarge','m4.2xlarge','m4.4xlarge','m4.10xlarge','m2.xlarge','m2.2xlarge','m2.4xlarge','cr1.8xlarge','r3.large','r3.xlarge','r3.2xlarge','r3.4xlarge','r3.8xlarge','x1.4xlarge','x1.8xlarge','x1.16xlarge','x1.32xlarge','i2.xlarge','i2.2xlarge','i2.4xlarge','i2.8xlarge','hi1.4xlarge','hs1.8xlarge','c1.medium','c1.xlarge','c3.large','c3.xlarge','c3.2xlarge','c3.4xlarge','c3.8xlarge','c4.large','c4.xlarge','c4.2xlarge','c4.4xlarge','c4.8xlarge','cc1.4xlarge','cc2.8xlarge','g2.2xlarge','g2.8xlarge','cg1.4xlarge','d2.xlarge','d2.2xlarge','d2.4xlarge','d2.8xlarge']


instECUMemoryDict = dict()

#------------------------------------------
# Internal functions
#------------------------------------------
def getCurPriceFromCloud(instList = EC2InstList):
    utcStr = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    productTypeList = ['Linux/UNIX', 'Linux/UNIX (Amazon VPC)']
    spotPriceDictForRegion = {}

    client = boto3.client('ec2', region_name = spotIndex_region)
    zoneList = EC2ZoneDict[spotIndex_region]
    
    for zoneSuffix in zoneList:
        spotPriceDictForZone = {}
        availZone =  spotIndex_region + zoneSuffix

        response = client.describe_spot_price_history(
            StartTime = utcStr, 
            EndTime = utcStr,
            InstanceTypes = instList,
            ProductDescriptions = productTypeList,
            AvailabilityZone = availZone,
        )
        
        # Check if we got a success response, else return
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            return spotPriceDictForRegion
        
        # Each instance-productType-zone combo comes as a seprate dictionary 
        # Process them one at a time from the list of responses
        for instPriceDict in response['SpotPriceHistory']:
            name = instPriceDict['InstanceType']
            isVpc = '.vpc' if 'VPC' in instPriceDict['ProductDescription'] else '' 
            price = instPriceDict['SpotPrice']
            spotPriceDictForZone[name + isVpc] = Decimal(price)

        spotPriceDictForRegion[availZone] = spotPriceDictForZone

    return spotPriceDictForRegion
#END getCurPriceFromCloud

def nth_root(val, n):
    ret = val**(1./n)
    return ret


def getECUMemoryInfo():
    global instECUMemoryDict
 
    instInfo = open("cpu-memory.info").read().splitlines()
    for line in instInfo:
        instType, cpu, ecu, memory = line.split('\t')
        weight = nth_root(float(ecu) * float(memory), 2)
        instECUMemoryDict[instType] = [float(cpu), float(ecu), float(memory), Decimal(weight)]
    
    #print instECUMemoryDict
    return
#END getECUMemoryInfo

def computeAverage(region, curSpotPriceDict):
    # Calculate the base running avg at time T
    regionTotal = Decimal(0.0)
    zoneTotal = Decimal(0.0)
    numMarkets = 0
    numZoneMarkets = 0
 
    for zone, zoneDict in curSpotPriceDict.items():
        zoneTotal = Decimal(0.0)
        numZoneMarkets = 0
        for inst, spotPrice in zoneDict.items():
            #inst.replace('.vpc','')
            if inst.endswith('.vpc'):
                inst = inst[:-4]
            curRate = spotPrice / instECUMemoryDict[inst][3]
            zoneTotal += curRate
            numZoneMarkets += 1
            regionTotal += curRate
            numMarkets += 1
        avgZoneVal = Decimal(zoneTotal/numZoneMarkets)
        print 'zone: ' + zone + '\t index-level = ' + str(round(avgZoneVal, 5))
 
    avgRegionVal = Decimal(regionTotal/numMarkets)
    print '----------------------------------'
    print 'region: ' + region + '\t index-level = ' + str(round(avgRegionVal, 5))

    return 
#END computeAverage
    
#------------------------------------------
# Public functions
# -- spotIndex_init
#------------------------------------------

def spotIndex_init(region):
    global spotIndex_region
    spotIndex_region = region
    return True
#END spotIndex_init


def spotIndex_getCurPrice(instList = EC2InstList):
    return getCurPriceFromCloud(instList)
#END spotIndex_getCurPrice


#------------------------------------------
# Unit test 
#------------------------------------------

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("region", help="AWS region")
    args = parser.parse_args()
    
    if spotIndex_init(args.region) == True:
        print 'Connected to ' + args.region
    
    startTime = datetime.now() 
    
    getECUMemoryInfo()
    spotPriceDictForRegion = spotIndex_getCurPrice(EC2InstList)
    computeAverage(args.region, spotPriceDictForRegion)
   
    queryTime = datetime.now() - startTime
    queryTimeMicrosec = (queryTime.seconds * 1000000) + queryTime.microseconds
    
    #print 'Query took ' + str(queryTimeMicrosec) + ' micro seconds'

