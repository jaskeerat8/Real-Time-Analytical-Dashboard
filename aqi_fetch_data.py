# Importing Libraries
import os, re, io
import json, time
import boto3
import requests
import s3fs
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import pytz
from dotenv import load_dotenv

# Load .env file
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)
load_dotenv()

# Credentials
aws_region = "ap-south-1"
weather_api_key = os.environ.get("weather_api_key")
aws_access_key_id = os.environ.get("aws_access_key_id")
aws_secret_access_key = os.environ.get("aws_secret_access_key")

# s3 Location
s3_path = "s3://github-projects-resume/Real_Time_Analytical_Dashboard"
raw_data_path = f"{s3_path}/data/raw"
final_data_path = f"{s3_path}/data/final"
pollutant_breakpoints = f"{s3_path}/resources/aqi_concentration_breakpoints.csv"

# Pollutants
pollutant_avg_period = {"pm2_5": "24", "pm10": "24", "so2": "24", "no2": "24", "co": "8", "o3": "8"}

def get_aqi_us():
    try_count = 0
    while try_count < 5:
        try:
            url = "https://www.aqi.in/in/dashboard/india/chandigarh"
            response = requests.get(url)
            html_content = response.text
            soup = BeautifulSoup(html_content, "html.parser")

            aqi_div = soup.find("div", class_=re.compile(".*aqi-value.*"))
            for span in aqi_div.find_all("span"):
                aqi_us = span.find(string=True, recursive=False).strip()
                if aqi_us.isdigit():
                    aqi_us = int(aqi_us)
                    break
            return aqi_us
        except:
            try_count = try_count + 1
            time.sleep(try_count * 60)
    return 0


def get_api_data():
    url = f"http://api.weatherapi.com/v1/current.json?key={weather_api_key}&q=Chandigarh&aqi=yes"
    response = requests.get(url, headers={"accept": "application/json"})
    weather_data = response.json()
    print(weather_data)

    current_time = datetime.fromtimestamp(weather_data["location"]["localtime_epoch"],
                                          pytz.timezone(weather_data["location"]["tz_id"]))
    temperature = round(weather_data["current"]["temp_c"])
    humidity = round(weather_data["current"]["humidity"])
    uv = round(weather_data["current"]["uv"])
    wind = round(weather_data["current"]["wind_kph"])
    wind_degree = round(weather_data["current"]["wind_degree"])

    pm2_5 = round(weather_data["current"]["air_quality"]["pm2_5"])
    pm10 = round(weather_data["current"]["air_quality"]["pm10"])
    so2 = round(weather_data["current"]["air_quality"]["so2"])
    co = round(weather_data["current"]["air_quality"]["co"] / 1000, 3)
    o3 = round(weather_data["current"]["air_quality"]["o3"])
    no2 = round(weather_data["current"]["air_quality"]["no2"])

    return current_time, temperature, humidity, uv, wind, wind_degree, pm2_5, pm10, so2, co, o3, no2


def raw_to_s3(df):
    s3_client = boto3.client("s3", region_name=aws_region, aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key)

    current_datetime = datetime.now(pytz.timezone("Asia/Kolkata"))
    file_location = f"{raw_data_path}/date={current_datetime.date()}/output_{current_datetime.strftime("%H_%M_%S")}.snappy.parquet"
    bucket_name = file_location.split("/")[2]
    key = "/".join(file_location.split("/")[3:])

    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=buffer.getvalue())
    return True


def read_s3():
    fs = s3fs.S3FileSystem(
        key=aws_access_key_id,
        secret=aws_secret_access_key,
        client_kwargs={'region_name': aws_region}
    )
    all_files = fs.glob(f"{raw_data_path}/**/*.parquet")

    df = pd.concat([pd.read_parquet(file, filesystem=fs) for file in all_files], ignore_index=True)
    df = df.drop_duplicates()
    df.sort_values(["timestamp"], ascending = [True], inplace=True)
    df.set_index('timestamp', drop=False, inplace=True)

    for pollutant, period in pollutant_avg_period.items():
        df[pollutant] = df[pollutant].rolling(f'{period}h').mean()
        df[pollutant] = df[pollutant].round().astype(int)
    df = df.reset_index(drop=True)
    return df


def calculate_aqi(row, breakpoint_df):
    aqi_in = 0
    for pollutant_key in pollutant_avg_period.keys():
        breakpoint_row = breakpoint_df[
            (breakpoint_df["pollutant"] == pollutant_key) &
            (breakpoint_df["low_concentration"] <= row[pollutant_key]) &
            (breakpoint_df["upper_concentration"] >= row[pollutant_key])
            ].iloc[0]

        c_low = breakpoint_row['low_concentration']
        c_high = breakpoint_row['upper_concentration']
        i_low = breakpoint_row['low_aqi']
        i_high = breakpoint_row['upper_aqi']

        aqi = ((i_high - i_low) / (c_high - c_low)) * (row[pollutant_key] - c_low) + i_low
        if aqi > aqi_in:
            aqi_in = int(round(aqi))
    return aqi_in


def final_to_s3(df):
    s3_client = boto3.client("s3", region_name=aws_region, aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key)

    file_location = f"{final_data_path}/output.csv"
    bucket_name = file_location.split("/")[2]
    key = "/".join(file_location.split("/")[3:])

    buffer = io.BytesIO()
    df.to_csv(buffer, index=False)
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=buffer.getvalue())
    return True


def lambda_handler(event=None, context=None):
    # AQI US Standard
    aqi_us = get_aqi_us()

    # Weather Data API
    current_time, temperature, humidity, uv, wind, wind_degree, pm2_5, pm10, so2, co, o3, no2 = get_api_data()

    # Write Raw Data
    raw_df = pd.DataFrame([{
        "timestamp": current_time,
        "aqi_us": aqi_us,
        "temperature": temperature,
        "humidity": humidity,
        "uv": uv,
        "wind": wind,
        "wind_degree": wind_degree,
        "pm2_5": pm2_5,
        "pm10": pm10,
        "so2": so2,
        "co": co,
        "o3": o3,
        "no2": no2
    }])
    print(raw_df)
    raw_to_s3(raw_df)

    time.sleep(30)

    # Write Final Data
    breakpoint_df = pd.read_csv(pollutant_breakpoints)
    df = read_s3()
    df["aqi_in"] = df.apply(lambda row: calculate_aqi(row, breakpoint_df), axis=1)
    print(df)
    final_to_s3(df)

    return {
        'statusCode': 200,
        'body': json.dumps('Data Processing Complete')
    }

if __name__ == '__main__':
    lambda_handler()