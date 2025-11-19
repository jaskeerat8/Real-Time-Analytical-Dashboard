# Importing Libraries
import os
import boto3
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
from flask import Flask
from flask_caching import Cache
from dotenv import load_dotenv


# Loading Environment
load_dotenv()

# Credentials
aws_region = "ap-south-1"
for key, value in os.environ.items():
    globals()[key] = value

# Image Folder
image_folder = "https://github-projects-resume.s3.ap-south-1.amazonaws.com/Real_Time_Analytical_Dashboard/resources/"


# Initialising Dash App
server = Flask(__name__)
app = Dash(__name__, server=server, external_stylesheets=["https://fonts.googleapis.com/css2?family=Poppins:wght@200;300;400;500;600;700&display=swap"])
app.title = "AQI Dashboard"
cache = Cache(app.server, config={"CACHE_TYPE": "SimpleCache",  "CACHE_DEFAULT_TIMEOUT": 300})


# Loading ML Model
with open("model.pkl", "rb") as f:
    model = pickle.load(f)

# Reading Data from s3
@cache.memoize()
def get_data():
    s3_data_path = "s3://github-projects-resume/Real_Time_Analytical_Dashboard/data/final/output.csv"
    bucket_name = s3_data_path.split("/")[2]
    file_key = "/".join(s3_data_path.split("/")[3:])

    s3_client = boto3.client("s3", region_name=aws_region, aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key)

    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    csv_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_content))
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=False)
    df = df.drop_duplicates("timestamp", keep="first")
    return df


# Defining Layout
app.layout = dmc.MantineProvider(
    children = html.Div(className="main_layout", children=[
        dcc.Interval(id="time_interval", interval=300000),
        html.Div(className="header", children=[
            dmc.Text("Chandigarh AQI", className="header_text"),
            html.Div(className="header_measure", children=[DashIconify(icon="fluent:temperature-16-filled", color="white", width=25), html.P(id="header_temperature")]),
            html.Div(className="header_measure", children=[DashIconify(icon="carbon:humidity", color="white", width=25), html.P(id="header_humidity")]),
            html.Div(className="header_measure", children=[DashIconify(icon="tabler:uv-index", color="white", width=25), html.P(id="header_uv")]),
            html.Div(className="header_measure", children=[DashIconify(icon="mi:wind", color="white", width=25), html.P(id="header_wind"),
                                                           html.Img(id="header_wind_direction", src=image_folder + "wind_arrow.png", alt="wind direction", width="20px")]),
            dmc.Text(className="header_date", id="header_date")
        ]),
        html.Div(className="aqi_line_chart_container", children=[
            html.Div(className="aqi_line_chart", children=dcc.Graph(id="aqi_line_chart", config={"displayModeBar": False, "responsive": True}, style={"width": "100%", "height": "100%"})),
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
                    html.Div(className="aqi_line_chart_legend_item", children=[html.B("Good (0-50)"), html.P("Ideal for all activities")], style={"background-color": "#377A07"}),
                    html.Div(className="aqi_line_chart_legend_item", children=[html.B("Satisfactory (51-100)"), html.P("Minor impact on health")], style={"background-color": "#9ACD32"}),
                    html.Div(className="aqi_line_chart_legend_item", children=[html.B("Moderate (101-200)"), html.P("Possible discomfort outdoors")], style={"background-color": "#FFC300"}),
                    html.Div(className="aqi_line_chart_legend_item", children=[html.B("Poor (201-300)"), html.P("Limit prolonged outdoor time")], style={"background-color": "#F58F09"}),
                    html.Div(className="aqi_line_chart_legend_item", children=[html.B("Very Poor (301-400)"), html.P("High risk for all")], style={"background-color": "#C41206"}),
                    html.Div(className="aqi_line_chart_legend_item", children=[html.B("Severe (401+)"), html.P("Serious hazards to health")], style={"background-color": "#810100"})
                ])
            ])
        ]),
        html.Div(className="aqi_measure_row", children=[
            html.Div(className="aqi_measure", children=[
                html.Div(className="aqi_measure_top", children=[
                    html.Div(className="aqi_measure_header", children=[DashIconify(icon="streamline:rain-cloud", color="black", width=25), html.P("PM2.5")]),
                    html.Div(className="aqi_measure_flag", id="aqi_measure_flag_pm25", children="Dominant Pollutant")
                ]),
                html.Div(className="aqi_measure_gauge_container", children=[
                    daq.Gauge(className="aqi_measure_gauge", id="pm25_gauge", showCurrentValue=True, digits=0, value=0, min=0, max=500, units="µg/m³",
                        color={"gradient": True, "ranges": {"#377A07": [0, 30], "#9ACD32": [30, 60], "#FFC300":[60, 90], "#F58F09": [90, 120], "#C41206":[120, 250], "#810100": [250, 500]}}
                    )
                ])
            ]),
            html.Div(className="aqi_measure", children=[
                html.Div(className="aqi_measure_top", children=[
                    html.Div(className="aqi_measure_header", children=[DashIconify(icon="streamline:rain-cloud-solid", color="black", width=25), html.P("PM10")]),
                    html.Div(className="aqi_measure_flag", id="aqi_measure_flag_pm10", children="Dominant Pollutant")
                ]),
                html.Div(className="aqi_measure_gauge_container", children=[
                    daq.Gauge(className="aqi_measure_gauge", id="pm10_gauge", showCurrentValue=True, digits=0, value=0, min=0, max=600, units="µg/m³",
                        color={"gradient": True, "ranges": {"#377A07": [0, 50], "#9ACD32": [50, 100], "#FFC300":[100, 250], "#F58F09": [250, 350], "#C41206":[350, 430], "#810100": [430, 600]}}
                    )
                ])
            ]),
            html.Div(className="aqi_measure", children=[
                html.Div(className="aqi_measure_top", children=[
                    html.Div(className="aqi_measure_header", children=[html.Img(src=image_folder + "Sulfur_Dioxide.png", alt="so2", width="30%"), html.P("Sulfur Dioxide")]),
                    html.Div(className="aqi_measure_flag", id="aqi_measure_flag_so2", children="Dominant Pollutant")
                ]),
                html.Div(className="aqi_measure_gauge_container", children=[
                    daq.Gauge(className="aqi_measure_gauge", id="so2_gauge", showCurrentValue=True, digits=0, value=0, min=0, max=2600, units="µg/m³",
                        color={"gradient": True, "ranges": {"#377A07": [0, 40], "#9ACD32": [40, 80], "#FFC300":[80, 380], "#F58F09": [380, 800], "#C41206":[800, 1600], "#810100": [1600, 2600]}}
                    )
                ])
            ]),
            html.Div(className="aqi_measure", children=[
                html.Div(className="aqi_measure_top", children=[
                    html.Div(className="aqi_measure_header", children=[html.Img(src=image_folder + "Carbon_Monoxide.png", alt="co", width="20%"), html.P("Carbon Monoxide")]),
                    html.Div(className="aqi_measure_flag", id="aqi_measure_flag_co", children="Dominant Pollutant")
                ]),
                html.Div(className="aqi_measure_gauge_container", children=[
                    daq.Gauge(className="aqi_measure_gauge", id="co_gauge", showCurrentValue=True, value=0, min=0, max=50, units="mg/m³",
                        color={"gradient": True, "ranges": {"#377A07": [0, 1], "#9ACD32": [1, 2], "#FFC300":[2, 10], "#F58F09": [10, 17], "#C41206":[17, 34], "#810100": [34, 50]}}
                    )
                ])
            ]),
            html.Div(className="aqi_measure", children=[
                html.Div(className="aqi_measure_top", children=[
                    html.Div(className="aqi_measure_header", children=[html.Img(src=image_folder + "Ozone.png", alt="o3", width="15%"), html.P("Ozone")]),
                    html.Div(className="aqi_measure_flag", id="aqi_measure_flag_o3", children="Dominant Pollutant")
                ]),
                html.Div(className="aqi_measure_gauge_container", children=[
                    daq.Gauge(className="aqi_measure_gauge", id="o3_gauge", showCurrentValue=True, digits=0, value=0, min=0, max=1000, units="µg/m³",
                        color={"gradient": True, "ranges": {"#377A07": [0, 50], "#9ACD32": [50, 100], "#FFC300":[100, 168], "#F58F09": [168, 208], "#C41206":[208, 748], "#810100": [748, 1000]}}
                    )
                ])
            ]),
            html.Div(className="aqi_measure", children=[
                html.Div(className="aqi_measure_top", children=[
                    html.Div(className="aqi_measure_header", children=[html.Img(src=image_folder + "Nitrogen_Dioxide.png", alt="no2", width="20%"), html.P("Nitrogen Dioxide")]),
                    html.Div(className="aqi_measure_flag", id="aqi_measure_flag_no2", children="Dominant Pollutant")
                ]),
                html.Div(className="aqi_measure_gauge_container", children=[
                    daq.Gauge(className="aqi_measure_gauge", id="no2_gauge", showCurrentValue=True, digits=0, value=0, min=0, max=800, units="µg/m³",
                        color={"gradient": True, "ranges": {"#377A07": [0, 40], "#9ACD32": [40, 80], "#FFC300":[80, 180], "#F58F09": [180, 280], "#C41206":[280, 400], "#810100": [400, 800]}}
                    )
                ])
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
    aqi_chart = px.line(df, x="timestamp", y=["aqi_in", "aqi_us"], template="plotly_white")

    custom_names = {"aqi_in": "INDIA Standard   ", "aqi_us": "US Standard   "}
    for trace in aqi_chart.data:
        trace.name = custom_names.get(trace.name, trace.name)

    aqi_chart.update_layout(autosize=True, margin=dict(l=0, r=25, b=0), height=350)
    aqi_chart.update_layout(title={"text": "<b>Air Quality Trend</b>", "x": 0.03, "y": 0.92, "yanchor": "top"}, title_font_color="#052F5F", title_font=dict(size=20, family="Poppins"))
    aqi_chart.update_layout(legend=dict(font=dict(color="#000000", size=14, family="Poppins"), orientation="h", x=0.99, y=1.15, xanchor="right", yanchor="top", title_text=""))
    aqi_chart.update_layout(xaxis_title="", yaxis_title="", legend_title_text="")
    aqi_chart.update_layout(yaxis_showgrid=True, yaxis_ticksuffix="  ", yaxis=dict(tickfont=dict(size=12, family="Poppins", color="black"), griddash="dash", gridwidth=1, gridcolor="#DADADA"))
    aqi_chart.update_layout(xaxis_showgrid=False, xaxis=dict(tickfont=dict(size=12, family="Poppins", color="#000000"), tickangle=0))
    aqi_chart.update_traces(mode="lines+markers", line=dict(width=2), marker=dict(sizemode="diameter", size=8, color="white", line=dict(width=2.5)))
    aqi_chart.update_yaxes(fixedrange=True)
    aqi_chart.update_xaxes(tickformat="%d %b'%y\n%I:%M%p", tickangle=0, tickmode="auto", automargin=True)

    # Hover Label
    aqi_chart.update_layout(hovermode="x unified", hoverlabel=dict(bgcolor="#c1dfff", font_size=12, font_family="Poppins", align="left"))
    aqi_chart.update_traces(hovertemplate="AQI Count: <b>%{y}</b><extra></extra>")
    return aqi_chart


# Updating AQI Measures
@app.callback(
    [Output("header_date", "children"), Output("aqi_reading_count_actual", "children"),
     Output("pm25_gauge", "value"), Output("pm10_gauge", "value"),
     Output("so2_gauge", "value"), Output("co_gauge", "value"),
     Output("o3_gauge", "value"), Output("no2_gauge", "value"),
     Output("header_temperature", "children"), Output("header_humidity", "children"),
     Output("header_uv", "children"), Output("header_wind", "children"),
    Output("header_wind_direction", "style")],
    Input("time_interval", "n_intervals")
)
def update_aqi_measures(time_interval):
    df = get_data()
    measures = df.iloc[-1]

    aqi = measures["aqi_24"]
    time_received = "Last Update (IST):\n" + measures["timestamp"].strftime("%d %B %Y, %I:%M %p")
    temperature = str(measures["temperature"]) + " °C"
    humidity = str(measures["humidity"]) + "%"
    wind = str(measures["wind"]) + " km/hr"
    wind_degree = {
        "transition": "transform 0.5s",
        "transform": f"rotate({measures['wind_degree']}deg)"
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

    return time_received, aqi, int(measures["pm2_5"]), int(measures["pm10"]), int(measures["so2"]), measures["co"], int(measures["o3"]), int(measures["no2"]), temperature, humidity, uv, wind, wind_degree


# Updating AQI Prominent Pollutant Flag
@app.callback(
    [Output("aqi_measure_flag_pm25", "style"), Output("aqi_measure_flag_pm10", "style"),
    Output("aqi_measure_flag_so2", "style"), Output("aqi_measure_flag_co", "style"),
    Output("aqi_measure_flag_o3", "style"), Output("aqi_measure_flag_no2", "style")],
    Input("time_interval", "n_intervals")
)
def update_prominent_pollutant_flag(time_interval):
    df = get_data()
    prominent_pollutant = df.iloc[-1]["prominent_pollutant"]

    hidden = {"display": "none"}
    visible = {"display": "block"}

    mapping = {"pm2_5": 0, "pm10": 1, "so2": 2, "co": 3, "o3": 4, "no2": 5}

    styles = [hidden] * 6
    index = mapping.get(prominent_pollutant)
    if index is not None:
        styles[index] = visible
    return styles


# Updating AQI Predicted Value
@app.callback(
    Output("aqi_reading_count_predicted", "children"),
    Input("time_interval", "n_intervals")
)
def update_aqi_predicted_count(time_interval):
    upcoming_hour = (datetime.now().astimezone(pytz.timezone("Asia/Kolkata"))+timedelta(hours=1)).hour
    aqi = model.predict(pd.DataFrame({"Hour": [upcoming_hour]}))[0]
    aqi = round(aqi)
    return aqi


# Running Main App
if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8000)