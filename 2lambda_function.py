# * Second Lambda function:-- (To stream from DynamoDB to new S3 bucket) ;

import boto3
import json
import os
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

# Set your environment variables
BUCKET_NAME = os.environ.get('S3_BUCKET', 'weatherdataprocessedd')
TABLE_NAME = os.environ.get('DYNAMODB_TABLE', 'WeatherData')

def lambda_handler(event, context):
    records = event.get('Records', [])
    if not records:
        return {
            'statusCode': 400,
            'body': 'No Records found in event'
        }

    table = dynamodb.Table(TABLE_NAME)
    processed_items = []

    for record in records:                                                                       # Loop over each stream record 
        if record['eventName'] == 'INSERT':
            new_image = record['dynamodb']['NewImage']
            item = {k: list(v.values())[0] for k, v in new_image.items()}

            # Build the new item by combining incoming data + your additional fields
            new_item = {
                'location_name': item.get('location_name', 'Unknown'),
                'localtime': item.get('localtime', datetime.utcnow().isoformat()),               # must include localtime
                'inserted_copy_time': datetime.utcnow().isoformat(),
                'original_temp_c': item.get('temp_c', 0),
                'original_humidity': item.get('humidity', 0),
                'dewpoint_c': item.get('dewpoint_c', ''),
                'condition_text': item.get('condition_text', ''),
                'pressure_mb': item.get('pressure_mb', ''),
                'country': item.get('country', ''),
                'cloud': item.get('cloud', ''),
                'feelslike_f': item.get('feelslike_f', ''),
                'uv_index': item.get('uv', ''),                                                 # assuming uv instead of uv_index field
                'condition_icon': item.get('condition_icon', ''),
                'wind_degree': item.get('wind_degree', ''),
                'visibility_miles': item.get('vis_miles', ''),
                'gust_mph': item.get('gust_mph', ''),
                'wind_dir': item.get('wind_dir', ''),
                'gust_kph': item.get('gust_kph', ''),
                'condition_code': item.get('condition_code', ''),
                'windchill_f': item.get('windchill_f', ''),
                'pressure_in': item.get('pressure_in', ''),
                'region': item.get('region', ''),
                'feelslike_c': item.get('feelslike_c', ''),
                'is_day': item.get('is_day', ''),
                'latitude': item.get('lat', ''),
                'temp_c': item.get('temp_c', ''),
                'temp_f': item.get('temp_f', ''),
                'windchill_c': item.get('windchill_c', ''),
                'wind_kph': item.get('wind_kph', ''),
                'wind_mph': item.get('wind_mph', ''),
                'heatindex_f': item.get('heatindex_f', ''),
                'precip_mm': item.get('precip_mm', ''),
                'longitude': item.get('lon', ''),
                'timezone': item.get('tz_id', ''),
                'heatindex_c': item.get('heatindex_c', ''),
                'visibility_km': item.get('vis_km', ''),
                'dewpoint_f': item.get('dewpoint_f', ''),
                'precip_in': item.get('precip_in', '')
            }

            # Insert the new item into DynamoDB
            table.put_item(Item=new_item)

            # Also upload original item to S3
            timestamp = datetime.utcnow().isoformat()
            file_name = f"dynamodb_record_{timestamp}.json"

            s3.put_object(                                                            # Upload the original flattened item to s3
                Bucket=BUCKET_NAME,
                Key=file_name,
                Body=json.dumps(item),
                ContentType='application/json'
            )

            processed_items.append(new_item)

    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Processed {len(processed_items)} records',
            'processed_data': processed_items
        })
    }

# ----------------------------------------------------------------------------
