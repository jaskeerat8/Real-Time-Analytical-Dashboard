# Importing Libraries
import io, os
import boto3
import requests
import pandas as pd
from datetime import *
import joblib
from xgboost import XGBRegressor
from dotenv import load_dotenv

# Loading Environment
load_dotenv()

# Credentials
aws_region = "ap-south-1"
for key, value in os.environ.items():
    globals()[key] = value

# s3 Location
s3_resource_path = "s3://github-projects-resume/Real_Time_Analytical_Dashboard/resources"
pollutant_breakpoints = f"{s3_resource_path}/aqi_concentration_breakpoints.csv"


def get_training_data():
    start_date = "2023-01-01"
    end_date = (datetime.now() - timedelta(days=1)).date()

    url = f"https://air-quality-api.open-meteo.com/v1/air-quality?latitude={lat}&longitude={lon}&hourly=pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,sulphur_dioxide&start_date={start_date}&end_date={end_date}"
    response = requests.get(url)

    df = pd.DataFrame(response.json()["hourly"])
    df = df.sort_values("time")
    df = df.dropna()

    df["time"] = pd.to_datetime(df["time"]) + timedelta(hours=5, minutes=30)
    df["month"] = df["time"].dt.month
    df["day"] = df["time"].dt.day
    df["hour"] = df["time"].dt.hour

    df["carbon_monoxide"] = df["carbon_monoxide"] / 1000
    df.rename(
        columns={"pm10": "pm10", "pm2_5": "pm2_5", "carbon_monoxide": "co", "nitrogen_dioxide": "no2", "ozone": "o3",
                 "sulphur_dioxide": "so2"}, inplace=True)
    return df


def calculate_aqi(row, breakpoint_df):
    aqi_breakpoint = 0
    for pollutant_key in ["pm10", "pm2_5", "co", "no2", "o3", "so2"]:

        pollutant_value = round(row[pollutant_key])
        breakpoint_pollutant_df = breakpoint_df[breakpoint_df["pollutant"] == pollutant_key]

        if pollutant_value < breakpoint_pollutant_df["low_concentration"].min():
            breakpoint_row = breakpoint_pollutant_df.iloc[0]
        elif pollutant_value > breakpoint_pollutant_df["upper_concentration"].max():
            breakpoint_row = breakpoint_pollutant_df.iloc[-1]
        else:
            breakpoint_row = breakpoint_pollutant_df[
                (breakpoint_pollutant_df["low_concentration"] <= pollutant_value) &
                (breakpoint_pollutant_df["upper_concentration"] >= pollutant_value)
                ].iloc[0]

        c_low = breakpoint_row["low_concentration"]
        c_high = breakpoint_row["upper_concentration"]
        i_low = breakpoint_row["low_aqi"]
        i_high = breakpoint_row["upper_aqi"]

        aqi = ((i_high - i_low) / (c_high - c_low)) * (row[pollutant_key] - c_low) + i_low
        if aqi > aqi_breakpoint:
            aqi_breakpoint = aqi
    return round(aqi_breakpoint, 2)


def model_training(df):
    df["aqi_lag1"] = df["aqi"].shift(1)
    df["aqi_lag2"] = df["aqi"].shift(2)
    df["aqi_lag3"] = df["aqi"].shift(3)

    df["target_aqi"] = df["aqi"].shift(-1)
    df = df.dropna()

    X = df[["pm10", "pm2_5", "co", "no2", "o3", "so2", "month", "day", "hour", "aqi_lag1", "aqi_lag2", "aqi_lag3"]]
    y = df["target_aqi"]

    best_params = {
        "subsample": 1.0,
        "reg_lambda": 0.1,
        "reg_alpha": 0.01,
        "n_estimators": 300,
        "min_child_weight": 5,
        "max_depth": 9,
        "learning_rate": 0.05,
        "gamma": 0.5,
        "colsample_bytree": 1.0,
        "objective": "reg:squarederror"
    }

    model = XGBRegressor(**best_params)
    model.fit(X, y)
    return model


def model_to_s3(model):
    buffer = io.BytesIO()
    joblib.dump(model, buffer)
    buffer.seek(0)

    s3_client = boto3.client("s3", region_name=aws_region, aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key)

    file_location = f"{s3_resource_path}/aqi_ml_model.pkl"
    bucket_name = file_location.split("/")[2]
    key = "/".join(file_location.split("/")[3:])

    s3_client.put_object(Bucket=bucket_name, Key=key, Body=buffer.getvalue())
    return True


def lambda_handler(event=None, context=None):
    # CPCB Breakpoint Ranges
    breakpoint_df = pd.read_csv(pollutant_breakpoints)

    # Collect Training Data
    df = get_training_data()

    # AQI Calculation
    df["aqi"] = df.apply(calculate_aqi, breakpoint_df=breakpoint_df, axis=1)

    # Model Training
    model = model_training(df)

    # Save to s3
    model_to_s3(model)


if __name__ == "__main__":
    lambda_handler()