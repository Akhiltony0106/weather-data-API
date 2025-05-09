# Weather-Data-Project 🌦️

This project collects real-time weather data from OpenWeather API, stores it in AWS DynamoDB and S3, and ingests it into Snowflake using Snowpipe.

## 🔧 Technologies Used
- AWS Lambda
- Amazon S3
- DynamoDB
- Snowflake
- Python
- SQL

## 📊 Architecture

![Architecture Diagram](Architecture-weatherAPI.jpg)

## 📁 Files
- `lambda_function.py`: Python Lambda code to fetch weather data
- `weather_pipeline_queries.sql`: Data ingestion & transformation queries
- `Taskpowerbi.pbix`: Power BI dashboard file for visualizing weather data
