import json
import os
import boto3


# Variables
TRACKER_NAME = 'Gas_Detector_Portable_Tracker'


def lambda_handler(event, context):
    client_loc = boto3.client('location')
    
    # Get positions
    response = client_loc.list_device_positions(TrackerName=TRACKER_NAME)
    #print('Location Response = {}'.format(response))
    #print('Entries = ', len(response['Entries']))

    # Build GeoJSON response payload
    payload = {
        "type": "FeatureCollection",
        "features": []
    }

    # Iterate through responses (not paginated)
    for entry in response['Entries']:

        try:
            _ = entry['Position'][0]
            _ = entry['Position'][1]
        except KeyError:
            continue

        payload['features'].append({
            "type": "Feature",
            "properties": {'DeviceId': entry['DeviceId']},
            "geometry": {
                "type": "Point",
                "coordinates": [
                    entry['Position'][0],
                    entry['Position'][1]
                ]
            }
        })

    #print('Entries with coords = ', len(payload['features']))

    return {
        'statusCode': 200,
        'body': json.dumps(payload)
    }
