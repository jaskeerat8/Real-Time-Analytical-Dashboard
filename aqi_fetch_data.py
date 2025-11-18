# Importing Libraries
import os, re, io
import json, time
import boto3
import requests
import s3fs
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import pytz
from dotenv import load_dotenv

# Load .env file
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)
load_dotenv()

# Credentials
aws_region = "ap-south-1"
for key, value in os.environ.items():
    globals()[key] = value

# s3 Location
s3_path = "s3://github-projects-resume/Real_Time_Analytical_Dashboard"
raw_data_path = f"{s3_path}/data/raw"
final_data_path = f"{s3_path}/data/final"
pollutant_breakpoints = f"{s3_path}/resources/aqi_concentration_breakpoints.csv"


# Molecular Weights
molecular_weights = {"pm25": 0, "pm10": 0, "no2": 46.01, "so2": 64.07, "co": 28.01, "o3": 48.00}

def calculate_aqi(df, breakpoint_df):
    aqi_breakpoint = 0
    prominent_pollutant = ""
    for pollutant_key in df.columns:
        breakpoint_row = breakpoint_df[
            (breakpoint_df["pollutant"] == pollutant_key) &
            (breakpoint_df["low_concentration"] <= df[pollutant_key].iloc[0]) &
            (breakpoint_df["upper_concentration"] >= df[pollutant_key].iloc[0])
            ].iloc[0]

        c_low = breakpoint_row['low_concentration']
        c_high = breakpoint_row['upper_concentration']
        i_low = breakpoint_row['low_aqi']
        i_high = breakpoint_row['upper_aqi']

        aqi = ((i_high - i_low) / (c_high - c_low)) * (df[pollutant_key].iloc[0] - c_low) + i_low
        if aqi > aqi_breakpoint:
            aqi_breakpoint = int(round(aqi))
            prominent_pollutant = pollutant_key
    return aqi_breakpoint, prominent_pollutant


def get_aqi_in(breakpoint_df):
    url = f"https://airquality.googleapis.com/v1/currentConditions:lookup?key={google_api_key}"
    headers = {"Content-Type": "application/json"}
    payload = {
        "universalAqi": False, "location": {"latitude": lat, "longitude": lon}, "extraComputations": ["POLLUTANT_CONCENTRATION"]
    }

    response = requests.post(url, json=payload, headers=headers)
    aqi_data = response.json()

    current_time = datetime.strptime(aqi_data["dateTime"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).astimezone(pytz.timezone("Asia/Kolkata"))
    pollutant_concentrations = {
        p["code"]: (
            p["concentration"]["value"] * (molecular_weights[p["code"]] / 24.45)
            if p["concentration"]["units"] == "PARTS_PER_BILLION"
            else p["concentration"]["value"]
        )
        for p in aqi_data["pollutants"]
    }

    pm2_5 = pollutant_concentrations.get("pm25", 0)
    pm10 = pollutant_concentrations.get("pm10", 0)
    no2 = pollutant_concentrations.get("no2", 0)
    so2 = pollutant_concentrations.get("so2", 0)
    co = pollutant_concentrations.get("co", 0) / 1000
    o3 = pollutant_concentrations.get("o3", 0)

    df = pd.DataFrame([{"pm2_5": pm2_5, "pm10": pm10, "so2": so2, "co": co, "o3": o3, "no2": no2}])
    aqi_in, prominent_pollutant = calculate_aqi(df, breakpoint_df)
    return current_time, aqi_in, pm2_5, pm10, so2, co, o3, no2, prominent_pollutant


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


def get_weather_data():
    url = f"http://api.weatherapi.com/v1/current.json?key={weather_api_key}&q=Chandigarh&aqi=no"
    response = requests.get(url, headers={"accept": "application/json"})
    weather_data = response.json()

    temperature = round(weather_data["current"]["temp_c"])
    humidity = round(weather_data["current"]["humidity"])
    uv = round(weather_data["current"]["uv"])
    wind = round(weather_data["current"]["wind_kph"])
    wind_degree = round(weather_data["current"]["wind_degree"])

    return temperature, humidity, uv, wind, wind_degree


def raw_to_s3(df):
    s3_client = boto3.client("s3", region_name=aws_region, aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key)

    current_datetime = datetime.now(pytz.timezone("Asia/Kolkata"))
    file_location = f"{raw_data_path}/date={current_datetime.date()}/output_{current_datetime.strftime('%H_%M_%S')}.snappy.parquet"
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

    valid_files = []
    for file in all_files:
        if fs.exists(file) and fs.info(file)['type'] == 'file' and file.endswith(".parquet"):
            valid_files.append(file)

    df = pd.concat([pd.read_parquet(file, filesystem=fs) for file in valid_files], ignore_index=True)
    df = df.drop_duplicates(subset=["timestamp", "aqi_in", "pm2_5", "pm10", "so2", "co", "o3", "no2"], keep='last')
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%d-%m-%Y %H:%M:%S")

    df = df[df["timestamp"] >= pd.Timestamp.now(tz="Asia/Kolkata") - pd.Timedelta(days=7)]
    df.sort_values(["timestamp"], ascending=True, inplace=True)
    df = df.reset_index(drop=True)
    return df


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
    # CPCB Breakpoint Ranges
    breakpoint_df = pd.read_csv(pollutant_breakpoints)

    # AQI India Standard
    current_time, aqi_in, pm2_5, pm10, so2, co, o3, no2, prominent_pollutant = get_aqi_in(breakpoint_df)

    # AQI US Standard
    aqi_us = get_aqi_us()

    # Weather Data API
    temperature, humidity, uv, wind, wind_degree = get_weather_data()

    # Write Raw Data to s3
    raw_df = pd.DataFrame([{
        "timestamp": current_time,
        "aqi_in": aqi_in,
        "pm2_5": pm2_5,
        "pm10": pm10,
        "so2": so2,
        "co": co,
        "o3": o3,
        "no2": no2,
        "aqi_us": aqi_us,
        "prominent_pollutant": prominent_pollutant,
        "temperature": temperature,
        "humidity": humidity,
        "uv": uv,
        "wind": wind,
        "wind_degree": wind_degree
    }])
    raw_to_s3(raw_df)

    # Read all Raw s3 Data
    time.sleep(120)
    df = read_s3()

    # Calculate AQI 24 hrs
    df_cpcb = df.copy()
    df_24hr_avg = df_cpcb.groupby("date", observed=True)[["pm2_5", "pm10", "so2", "no2"]].mean()

    df_cpcb = df_cpcb.set_index("timestamp").sort_index()
    df_cpcb["co"] = df_cpcb["co"].rolling("8h").mean()
    df_cpcb["o3"] = df_cpcb["o3"].rolling("8h").mean()
    df_8hr_max = df_cpcb.resample("D")[["co", "o3"]].max()

    df_24hr_avg.index = pd.to_datetime(df_24hr_avg.index).date
    df_8hr_max.index = pd.to_datetime(df_8hr_max.index).date
    final_df = pd.merge(df_24hr_avg, df_8hr_max, left_index=True, right_index=True, how='inner')
    final_df = final_df.loc[[final_df.index.max()]]
    aqi_24, prominent= calculate_aqi(final_df, breakpoint_df)
    df["aqi_24"] = aqi_24

    # Write Final Data
    final_to_s3(df)

    return {
        'statusCode': 200,
        'body': json.dumps('Data Processing Complete')
    }

if __name__ == '__main__':
    lambda_handler()