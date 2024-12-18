# Importing Libraries
import os
import requests
import mysql.connector
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# Inserting Data into MySQL
def post_data(time_received, aqi_us_count, aqi_in_count, pm25, pm10, so2, co, o3, no2, temperature, humidity, uv, wind):
    print(time_received, aqi_us_count, aqi_in_count, pm25, pm10, so2, co, o3, no2, temperature, humidity, uv, wind)
    mysql_config = {
        "host": os.getenv("db_host"),
        "port": os.getenv("db_port"),
        "database": os.getenv("db_database"),
        "user": os.getenv("db_user"),
        "password": os.getenv("db_password")
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
        print("New Data Inserted")
    else:
        print("Data already Exists")

    mysql_cursor.close()
    mysql_connection.close()
    return True

# Main Function
def data_scraping():
    #Scraping Data
    url = "https://www.iqair.com/au/india/chandigarh"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, "html.parser")
    time_received = soup.find("time")["datetime"]
    time_received = datetime.strptime(time_received, "%Y-%m-%dT%H:%M:%S.%fZ") + timedelta(hours=5, minutes=30)
    aqi_pollutant_table = soup.find(class_="aqi-overview-detail__other-pollution-table")
    pm25, pm10, o3, no2, so2, co = [int(float(value.text)) for value in aqi_pollutant_table.find_all(class_="pollutant-concentration-value")]
    aqi_us_count = int(soup.find(class_="aqi-value__value").text)

    url = "https://aqicn.org/city/india/chandigarh/sector-25/"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    aqi_in_count = int(soup.find(class_="aqivalue").text)

    #Weather Data API
    url = f"""http://api.weatherapi.com/v1/current.json?key={os.getenv('weather_api_key')}&q=Chandigarh&aqi=no"""
    response = requests.get(url, headers={"accept": "application/json"})
    weather_data = response.json()
    temperature = round(weather_data["current"]["temp_c"])
    humidity = round(weather_data["current"]["humidity"])
    uv = round(weather_data["current"]["uv"])
    wind = round(weather_data["current"]["wind_kph"])

    #Post Data to MySQL
    post_data(time_received, aqi_us_count, aqi_in_count, pm25, pm10, so2, co, o3, no2, temperature, humidity, uv, wind)

if __name__ == "__main__":
    data_scraping()
    print("Data Scraping and Insertion Completed Successfully.")
