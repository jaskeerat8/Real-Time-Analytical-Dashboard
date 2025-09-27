# Importing Libraries
import os
import boto3
import statistics
import pickle
import pandas as pd
from io import StringIO
import pytz
from datetime import datetime, timedelta
import dash_daq as daq
import plotly.express as px
import dash_mantine_components as dmc
from dash_iconify import DashIconify
from dash import Dash, html, dcc, Input, Output
from dotenv import load_dotenv

# Loading Environment
load_dotenv()

# Credentials
aws_region = "ap-south-1"
weather_api_key = os.environ.get("weather_api_key")
aws_access_key_id = os.environ.get("aws_access_key_id")
aws_secret_access_key = os.environ.get("aws_secret_access_key")

# Loading ML Model
with open("model.pkl", "rb") as f:
    model = pickle.load(f)

# Reading Data from Lambda
def get_data():
    s3_data_path = "s3://github-projects-resume/Real_Time_Analytical_Dashboard/data/final/output.csv"
    bucket_name = s3_data_path.split("/")[2]
    file_key = "/".join(s3_data_path.split("/")[3:])

    s3_client = boto3.client("s3", region_name=aws_region, aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key)

    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    csv_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_content))
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

# Defining Layout
app = Dash(__name__, external_stylesheets=["https://fonts.googleapis.com/css2?family=Poppins:wght@200;300;400;500;600;700&display=swap"])
server = app.server
app.title = "AQI Measure"
app.layout = dmc.MantineProvider(
    children = html.Div(className="main_layout", children=[
        dcc.Interval(id="time_interval", interval=300000),
        dcc.Interval(id="model_time_interval", interval=1800000),
        html.Div(className="header", children=[
            dmc.Text("Chandigarh AQI", className="header_text"),
            html.Div(className="header_measure", children=[DashIconify(icon="fluent:temperature-16-filled", color="white", width=25), html.P(id="header_temperature")]),
            html.Div(className="header_measure", children=[DashIconify(icon="carbon:humidity", color="white", width=25), html.P(id="header_humidity")]),
            html.Div(className="header_measure", children=[DashIconify(icon="tabler:uv-index", color="white", width=25), html.P(id="header_uv")]),
            html.Div(className="header_measure", children=[DashIconify(icon="mi:wind", color="white", width=25), html.P(id="header_wind"),
                html.Img(id="header_wind_direction", src="https://github-projects-resume.s3.ap-south-1.amazonaws.com/Real_Time_Analytical_Dashboard/resources/wind_arrow.png", alt="wind direction", width="20px")]),
            dmc.Text(className="header_date", id="header_date")
        ]),
        html.Div(className="aqi_line_chart_container", children=[
            html.Div(className="aqi_line_chart", children=dcc.Graph(id="aqi_line_chart", config={"displayModeBar": False})),
            html.Div(className="aqi_info", children=[
                html.Div(className="aqi_reading_container", children=[
                    html.Div(className="aqi_reading", children=[
                        html.P(className="aqi_reading_heading", children="Current AQI"),
                        html.P(className="aqi_reading_count_actual", id="aqi_reading_count_actual"),
                        html.P(className="aqi_reading_subheader", children="(24 Hour Average)")
                    ]),
                    html.Div(className="aqi_reading", children=[
                        html.P(className="aqi_reading_heading", children="Next ML Predicted AQI"),
                        html.P(className="aqi_reading_count_predicted", id="aqi_reading_count_predicted"),
                        html.P(className="aqi_reading_subheader", children="(Per Hour)")
                    ])
                ]),
                html.Div(className="aqi_line_chart_legend", children=[
                    html.Div(className="aqi_line_chart_legend_header", children="Air Quality Index"),
                    html.Div(className="aqi_line_chart_legend_item", children="Good (0-50)", style={"background-color": "#377A07"}),
                    html.Div(className="aqi_line_chart_legend_item", children="Satisfactory (51-100)", style={"background-color": "#9ACD32"}),
                    html.Div(className="aqi_line_chart_legend_item", children="Moderate (101-200)", style={"background-color": "#FFC300"}),
                    html.Div(className="aqi_line_chart_legend_item", children="Poor (201-300)", style={"background-color": "#F58F09"}),
                    html.Div(className="aqi_line_chart_legend_item", children="Very Poor (301-400)", style={"background-color": "#C41206"}),
                    html.Div(className="aqi_line_chart_legend_item", children="Severe (401+)", style={"background-color": "#810100"})
                ])
            ])
        ]),
        html.Div(className="aqi_measure_row", children=[
            html.Div(className="aqi_measure", children=[
                html.Div(className="aqi_measure_header", children=[DashIconify(icon="streamline:rain-cloud", color="black", width=25), html.P("PM2.5")]),
                daq.Gauge(className="aqi_measure_gauge", id="PM25_gauge", showCurrentValue=True, value=0, size=170, min=0, max=500, units="ug/m³",
                    color={"gradient": True, "ranges": {"#377A07": [0, 30], "#9ACD32": [30, 60], "#FFC300":[60, 90], "#F58F09": [90, 120], "#C41206":[120, 250], "#810100": [250, 500]}}
                )
            ]),
            html.Div(className="aqi_measure", children=[
                html.Div(className="aqi_measure_header", children=[DashIconify(icon="streamline:rain-cloud-solid", color="black", width=25), html.P("PM10")]),
                daq.Gauge(className="aqi_measure_gauge", id="PM10_gauge", showCurrentValue=True, value=0, size=170, min=0, max=700, units="ug/m³",
                    color={"gradient": True, "ranges": {"#377A07": [0, 50], "#9ACD32": [50, 100], "#FFC300":[100, 250], "#F58F09": [250, 350], "#C41206":[350, 430], "#810100": [430, 700]}}
                )
            ]),
            html.Div(className="aqi_measure", children=[
                html.Div(className="aqi_measure_header", children=[html.Img(src="https://github-projects-resume.s3.ap-south-1.amazonaws.com/Real_Time_Analytical_Dashboard/resources/Sulfur_Dioxide.png", alt="so2", width="30%"), html.P("Sulfur Dioxide")]),
                daq.Gauge(className="aqi_measure_gauge", id="so2_gauge", showCurrentValue=True, value=0, size=170, min=0, max=2600, units="ug/m³",
                    color={"gradient": True, "ranges": {"#377A07": [0, 40], "#9ACD32": [40, 80], "#FFC300":[80, 380], "#F58F09": [380, 800], "#C41206":[800, 1600], "#810100": [1600, 2600]}}
                )
            ]),
            html.Div(className="aqi_measure", children=[
                html.Div(className="aqi_measure_header", children=[html.Img(src="https://github-projects-resume.s3.ap-south-1.amazonaws.com/Real_Time_Analytical_Dashboard/resources/Carbon_Monoxide.png", alt="co", width="20%"), html.P("Carbon Monoxide")]),
                daq.Gauge(className="aqi_measure_gauge", id="co_gauge", showCurrentValue=True, value=0, size=170, min=0, max=100, units="mg/m³",
                    color={"gradient": True, "ranges": {"#377A07": [0, 1], "#9ACD32": [1, 2], "#FFC300":[2, 10], "#F58F09": [10, 17], "#C41206":[17, 34], "#810100": [34, 100]}}
                )
            ]),
            html.Div(className="aqi_measure", children=[
                html.Div(className="aqi_measure_header", children=[html.Img(src="https://github-projects-resume.s3.ap-south-1.amazonaws.com/Real_Time_Analytical_Dashboard/resources/Ozone.png", alt="o3", width="15%"), html.P("Ozone")]),
                daq.Gauge(className="aqi_measure_gauge", id="o3_gauge", showCurrentValue=True, value=0, size=170, min=0, max=1000, units="ug/m³",
                    color={"gradient": True, "ranges": {"#377A07": [0, 50], "#9ACD32": [50, 100], "#FFC300":[100, 168], "#F58F09": [168, 208], "#C41206":[208, 748], "#810100": [748, 1000]}}
                )
            ]),
            html.Div(className="aqi_measure", children=[
                html.Div(className="aqi_measure_header", children=[html.Img(src="https://github-projects-resume.s3.ap-south-1.amazonaws.com/Real_Time_Analytical_Dashboard/resources/Nitrogen_Dioxide.png", alt="no2", width="20%"), html.P("Nitrogen Dioxide")]),
                daq.Gauge(className="aqi_measure_gauge", id="no2_gauge", showCurrentValue=True, value=0, size=170, min=0, max=800, units="ug/m³",
                    color={"gradient": True, "ranges": {"#377A07": [0, 40], "#9ACD32": [40, 80], "#FFC300":[80, 180], "#F58F09": [180, 280], "#C41206":[280, 400], "#810100": [400, 800]}}
                )
            ])
        ]),
        html.Div(className="footer", children=[
            dmc.Text("Following Indian Central Pollution Control Board (CPCB) Standard", className="footer_text")
        ])
    ])
)


# Updating AQI Line Chart
@app.callback(
    Output("aqi_line_chart", "figure"),
    Input("time_interval", "n_intervals")
)
def update_aqi_line_chart(time_interval):
    df = get_data()
    df = df.rename(columns={"aqi_us": "AQI_US", "aqi_in": "AQI_IN"})

    aqi_chart = px.line(df, x="timestamp", y=["AQI_US", "AQI_IN"], template="plotly_white")
    aqi_chart.update_layout(margin=dict(l=0, r=0, b=0, t=0), height=350)
    aqi_chart.update_layout(legend=dict(font=dict(color="#000000", size=14, family="Poppins"), traceorder="grouped", orientation="h", x=0.993, y=1, xanchor="right", yanchor="bottom", title_text=""))
    aqi_chart.update_layout(xaxis_title="", yaxis_title="", legend_title_text="")
    aqi_chart.update_layout(yaxis_showgrid=True, yaxis_ticksuffix="  ", yaxis=dict(dtick=25, tickfont=dict(size=12, family="Poppins", color="black"), griddash="dash", gridwidth=1, gridcolor="#DADADA"))
    aqi_chart.update_layout(xaxis_showgrid=False, xaxis=dict(tickfont=dict(size=12, family="Poppins", color="#000000"), tickangle=0))
    aqi_chart.update_traces(mode="lines+markers", line=dict(width=2), marker=dict(sizemode="diameter", size=8, color="white", line=dict(width=2.5)))
    aqi_chart.update_yaxes(fixedrange=True)
    aqi_chart.update_xaxes(tickformat="%d %B %Y\n%I:%M%p")

    # Hover Label
    aqi_chart.update_layout(hovermode="x unified", hoverlabel=dict(bgcolor="#c1dfff", font_size=12, font_family="Poppins", align="left"))
    aqi_chart.update_traces(hovertemplate="AQI Count: <b>%{y}</b><extra></extra>")
    return aqi_chart


# Updating AQI Measures
@app.callback([Output("header_date", "children"),
     Output("aqi_reading_count_actual", "children"),
     Output("PM25_gauge", "value"),
     Output("PM10_gauge", "value"),
     Output("so2_gauge", "value"),
     Output("co_gauge", "value"),
     Output("o3_gauge", "value"),
     Output("no2_gauge", "value"),
     Output("header_temperature", "children"),
     Output("header_humidity", "children"),
     Output("header_uv", "children"),
     Output("header_wind", "children"),
    Output("header_wind_direction", "style")],
    Input("time_interval", "n_intervals")
)
def update_aqi_measures(time_interval):
    df = get_data()
    measures = df.iloc[-1]
    aqi_us_count = measures["aqi_us"]
    aqi_in_count = measures["aqi_in"]
    aqi = round(statistics.mean([aqi_us_count, aqi_in_count]))

    time_received = "Last Update (IST):\n" + measures["timestamp"].strftime("%d %B %Y, %I:%M %p")
    temperature = str(measures["temperature"]) + " °C"
    humidity = str(measures["humidity"]) + "%"
    wind = str(measures["wind"]) + " km/hr"
    wind_degree = {
        "transition": "transform 0.5s",
        "transform": f"rotate({measures["wind_degree"]}deg)"
    }

    uv = measures["uv"]
    if(uv > 11):
        uv = str(uv) + " (Extreme)"
    elif(uv > 8):
        uv = str(uv) + " (Very High)"
    elif(uv > 6):
        uv = str(uv) + " (High)"
    elif(uv > 3):
        uv = str(uv) + " (Moderate)"
    else:
        uv = str(uv) + " (Low)"

    return time_received, aqi, measures["pm2_5"], measures["pm10"], measures["so2"], measures["co"], measures["o3"], measures["no2"], temperature, humidity, uv, wind, wind_degree


# Updating AQI Predicted Value
@app.callback(
    Output("aqi_reading_count_predicted", "children"),
    Input("model_time_interval", "n_intervals")
)
def update_aqi_predicted_count(time_interval):
    upcoming_hour = (datetime.now().astimezone(pytz.timezone("Asia/Kolkata"))+timedelta(hours=1)).hour
    aqi = model.predict(pd.DataFrame({"Hour": [upcoming_hour]}))[0]
    aqi = round(aqi)
    return aqi


# Running Main App
if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8000)