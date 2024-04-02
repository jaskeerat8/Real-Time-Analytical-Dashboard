# Importing Libraries
import os
import math
import json
import boto3
import requests
import mysql.connector
from bs4 import BeautifulSoup
from datetime import datetime

# Inserting Data into MySQL
def post_data(value, time_received, aqi_us_count, aqi_in_count, pm25, pm10, so2, co, o3, no2, temperature, humidity, uv, wind):
    mysql_config = {
        "host": value["endpoint"],
        "user": value["user"],
        "password": value["password"],
        "database": value["database"]
    }
    mysql_connection = mysql.connector.connect(**mysql_config)
    mysql_cursor = mysql_connection.cursor()

    # Checking if Data exists in MySQL
    if mysql_connection.is_connected():
        already_exist_query = """
        SELECT COUNT(*) FROM dashboard.aqi_measures 
        WHERE time_received = %s
        """
        mysql_cursor.execute(already_exist_query, (time_received,))
        count = mysql_cursor.fetchone()[0]
    
    # Insert Data into MySQL
    if(count == 0):
        insert_query = f"""
        INSERT INTO dashboard.aqi_measures 
        (time_received, aqi_us_count, aqi_in_count, pm25, pm10, so2, co, o3, no2, temperature, humidity, uv, wind) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        mysql_cursor.execute(insert_query, (time_received, aqi_us_count, aqi_in_count, pm25, pm10, so2, co, o3, no2, temperature, humidity, uv, wind))
        mysql_connection.commit()
    else:
        print("Data already Exists")
    
    mysql_cursor.close()
    mysql_connection.close()
    return True

# Main Function
def lambda_handler(event, context):
    region_name = "ap-southeast-2"
    secret_name = "rdsMYSQL"
    session = boto3.session.Session(region_name=region_name, aws_access_key_id=os.environ.get("aws_access_key_id"),
                                    aws_secret_access_key=os.environ.get("aws_secret_access_key"))
    sm_client = session.client(service_name="secretsmanager")
    
    try:
        get_secret_value_response = sm_client.get_secret_value(SecretId=secret_name)
        value = json.loads(get_secret_value_response["SecretString"])
    except Exception as e:
        print("Failed to Read Data:", e)
    
    #Scraping Data
    url = "https://www.aqi.in/au/dashboard/india/chandigarh"
    response = requests.get(url)
    html_content = response.text
    soup = BeautifulSoup(html_content, "html.parser")
    time_received = soup.find(class_="card-location-time").text.split(":", 1)[-1].strip()
    time_received = datetime.strptime(time_received, "%d %b %Y, %I:%M%p")
    
    aqi_us_count = int(soup.find_all(class_="AQI_toggle aqiUsa")[-1].text)
    aqi_in_count = int(soup.find_all(class_="AQI_toggle aqiInd")[-1].text)
    pm25, pm10, so2, co, o3, no2 = [int(value.text) for value in soup.find_all(class_="Pollutants_sensor_text")]
    
    #Weather Data API
    url = f"""https://api.tomorrow.io/v4/weather/realtime?location=chandigarh&apikey={os.environ.get("weather_api_key")}"""
    response = requests.get(url, headers={"accept": "application/json"})
    weather_data = response.json()
    temperature = math.ceil(weather_data["data"]["values"]["temperatureApparent"])
    humidity = math.ceil(weather_data["data"]["values"]["humidity"])
    uv = weather_data["data"]["values"]["uvIndex"]
    wind = math.ceil(weather_data["data"]["values"]["windGust"] * 1.60934)
    
    #Post Data to MySQL
    post_data(value, time_received, aqi_us_count, aqi_in_count, pm25, pm10, so2, co, o3, no2, temperature, humidity, uv, wind)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
