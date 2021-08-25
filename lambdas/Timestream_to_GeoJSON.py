import json
import os
import datetime
import boto3
import random
import time


# Variables
TRACKER_NAME = 'Gas_Detector_Portable_Tracker'


def lambda_handler(event, context):
    #print('event={}'.format(event))
    #print('context={}'.format(context))

    # Check to see if API call ahs a query string for "duration", which is how long in the past to query position trails.  If missing, default to 10 minutes
    try:
        history_duration = int(event['queryStringParameters']['duration'])
    except KeyError:
        history_duration = 600
    
    # Build Timestream query
    query_string = 'SELECT DeviceID, time, measure_name, measure_value::double, measure_value::bigint FROM "Gas_Detector_Portable_TDB"."Positions" WHERE time BETWEEN ago({}s) AND now() ORDER BY time DESC'.format(history_duration)
    print('query={}'.format(query_string))
    
    # Reset dictionary (gets cached if Lambda called often, causing response to grow out of control)
    device_data = {}
    
    client_timestream = boto3.client('timestream-query')
    response = client_timestream.query(QueryString=query_string)
    
    #print(json.dumps(response)[:1000])
    #print('rows = ', len(response['Rows']))
    
    # Process TimeStream response
    # Data stored in TimeStream:
    # 0. timestamp
    # 1. value name
    # 2. latitude/longitude (float)
    # 3. gas readings (int)
    for i in range(0, len(response['Rows'])):
        #print(response['Rows'][i]['Data'][1]['ScalarValue'])
        device_id = response['Rows'][i]['Data'][0]['ScalarValue']
        timestamp = response['Rows'][i]['Data'][1]['ScalarValue']
        name      = response['Rows'][i]['Data'][2]['ScalarValue']
        
        if device_data.get(device_id) is None:
            device_data.update({device_id: {}})
        
        if device_data[device_id].get(timestamp) is None:
            #print('Timestamp being added: {}.'.format(timestamp))
            device_data[device_id].update({timestamp: {}})
            #print('adding device = {} and timestamp = {}'.format(device_id, timestamp))
            #print('device_data[{}]= {}'.format(i, device_data[device_id]))
        
        if name == 'gas_reading':
            gas_reading = response['Rows'][i]['Data'][4]['ScalarValue']
            #print('Gas reading being added: {}.'.format(gas_reading))
            device_data[device_id][timestamp].update({'gas_reading':gas_reading})
            #print('device_data[{}][{}] = {}'.format(i, device_data[device_id], device_data[device_id][timestamp]))

        elif name == 'latitude':
            latitude = response['Rows'][i]['Data'][3]['ScalarValue']
            device_data[device_id][timestamp].update({'latitude':latitude})

        elif name == 'longitude':
            longitude = response['Rows'][i]['Data'][3]['ScalarValue']
            device_data[device_id][timestamp].update({'longitude':longitude})

        #print('device_data[{}][{}] = {}'.format(i, device_id, device_data[device_id][timestamp]))

    # Build GeoJSON payload to return
    payload = {
        "type": "FeatureCollection",
        "features": [
        ]
    }
    
    #print('device_data length = ', len(device_data))
    #for device_id in device_data:
        #print('number of timestamps = {}'.format(len(device_data[device_id])))
        #print('device_data[device_id] = ', device_data[device_id])
    
    # Convert data to GeoJSON
    for device_id in device_data:
        #print('device = {} has timestamps = {}'.format(device_id, len(device_data[device_id])))
        for timestamp in device_data[device_id]:
            try: 
                # Check to see if variables are present, catch ther KeyError if not
                _ = device_data[device_id][timestamp]['longitude']
                _ = device_data[device_id][timestamp]['latitude']
                
                # Generate GeoJSON feature
                feature = {
                    "type": "Feature",
                    "properties": {
                        'DeviceId': device_id,
                        'gas_reading': int(device_data[device_id][timestamp]['gas_reading']),
                        'timestamp': timestamp
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": [
                            float(device_data[device_id][timestamp]['longitude']),
                            float(device_data[device_id][timestamp]['latitude'])
                        ]
                    }
                }
                
                # Append data to payload
                payload['features'].append(feature)
    
            except KeyError as e:
                print('KeyError')
                pass
        
    #print('payload = {}'.format(payload))
    
    return {
        'statusCode': 200,
        'body': json.dumps(payload)
    }
