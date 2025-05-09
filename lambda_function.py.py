# * First Lambda function:-(API to s3 bucket and DynamoDB ;

import json
import boto3
import os
import urllib.request
from datetime import datetime

# AWS clients
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

# Environment variables
WEATHER_API_KEY = os.environ.get('apikey', 'a9f1f6c93bf24a41891102852250904')
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE', 'WeatherData')
S3_BUCKET = os.environ.get('S3_BUCKET', 'akhil-weatherdata')

# List of multiple locations to fetch
LOCATIONS = ['Bangalore', 'Kolkata', 'Mumbai', 'Delhi', 'Chennai']

# DynamoDB table
table = dynamodb.Table(DYNAMODB_TABLE)

def lambda_handler(event, context):
    try:
        results = []

        for location in LOCATIONS:
            weather_api_url = f'http://api.weatherapi.com/v1/current.json?key={WEATHER_API_KEY}&q={location}'
            print(f"Fetching weather for {location}...")

            with urllib.request.urlopen(weather_api_url) as response:                                      # open url connection              
                weather_data = json.loads(response.read().decode())                                        # parse the JSON response

            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")                                        # generate a UTC timestamp

            item = {
                'location_name': weather_data['location']['name'],
                'localtime': weather_data['location']['localtime'],
                'region': weather_data['location']['region'],
                'country': weather_data['location']['country'],
                'latitude': str(weather_data['location']['lat']),
                'longitude': str(weather_data['location']['lon']),
                'timezone': weather_data['location']['tz_id'],
                'temp_c': str(weather_data['current']['temp_c']),
                'temp_f': str(weather_data['current']['temp_f']),
                'is_day': str(weather_data['current']['is_day']),
                'condition_text': weather_data['current']['condition']['text'],
                'condition_icon': weather_data['current']['condition']['icon'],
                'condition_code': str(weather_data['current']['condition']['code']),
                'wind_mph': str(weather_data['current']['wind_mph']),
                'wind_kph': str(weather_data['current']['wind_kph']),
                'wind_degree': str(weather_data['current']['wind_degree']),
                'wind_dir': weather_data['current']['wind_dir'],
                'pressure_mb': str(weather_data['current']['pressure_mb']),
                'pressure_in': str(weather_data['current']['pressure_in']),
                'precip_mm': str(weather_data['current']['precip_mm']),
                'precip_in': str(weather_data['current']['precip_in']),
                'humidity': str(weather_data['current']['humidity']),
                'cloud': str(weather_data['current']['cloud']),
                'feelslike_c': str(weather_data['current']['feelslike_c']),
                'feelslike_f': str(weather_data['current']['feelslike_f']),
                'windchill_c': str(weather_data['current'].get('windchill_c', 0)),
                'windchill_f': str(weather_data['current'].get('windchill_f', 0)),
                'heatindex_c': str(weather_data['current'].get('heatindex_c', 0)),
                'heatindex_f': str(weather_data['current'].get('heatindex_f', 0)),
                'dewpoint_c': str(weather_data['current'].get('dewpoint_c', 0)),
                'dewpoint_f': str(weather_data['current'].get('dewpoint_f', 0)),
                'visibility_km': str(weather_data['current']['vis_km']),
                'visibility_miles': str(weather_data['current']['vis_miles']),
                'uv_index': str(weather_data['current']['uv']),
                'gust_mph': str(weather_data['current']['gust_mph']),
                'gust_kph': str(weather_data['current']['gust_kph']),
                'inserted_at': datetime.utcnow().isoformat()                                             # store current UTC timestamp for insertion
            } 

            # Insert into DynamoDB
            table.put_item(Item=item)
            print(f"Inserted {location} weather data into DynamoDB.")

            # Upload to S3
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=f"weather_conditions_{location}_{timestamp}.json",
                Body=json.dumps(weather_data),
                ContentType='application/json'
            )
            print(f"Uploaded {location} weather data to S3.")

            results.append({'location': location, 'status': 'success'})

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Weather data for all locations saved successfully!',
                'results': results
            })
        }

    except Exception as e:
        print(f"Error occurred: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
# ----------------------------------------------------------------------------------
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
