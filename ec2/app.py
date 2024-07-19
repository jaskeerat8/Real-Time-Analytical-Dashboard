# Importing Libraries
import os
import json
import statistics
import pandas as pd
from sqlalchemy import create_engine
import pytz
from datetime import datetime, timedelta
import dash_daq as daq
import plotly.express as px
import dash_mantine_components as dmc
from dash_iconify import DashIconify
from dash import Dash, html, dcc, Input, Output
import pickle

# Loading ML Model
# with open("model.pkl", "rb") as f:
#     model = pickle.load(f)

def get_data():
    mysql_host = os.getenv("db_host")
    mysql_user = os.getenv("db_user")
    mysql_password = os.getenv("db_password")
    mysql_db = os.getenv("db_database")
    print(mysql_host, mysql_user, mysql_password, mysql_db)
    # Reading the Data
    mysql_connection = f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_db}"
    mysql_engine = create_engine(mysql_connection)
    df = pd.read_sql("SELECT * FROM aqi_measures WHERE time_received >= DATE_SUB(CURDATE(), INTERVAL 1 DAY) ORDER BY time_received DESC", con=mysql_engine)
    mysql_engine.dispose()
    return df

# Defining Layout
app = Dash(__name__, external_stylesheets=["https://fonts.googleapis.com/css2?family=Poppins:wght@200;300;400;500;600;700&display=swap"])
server = app.server
app.title = "AQI Measure"
app.layout = html.Div(className="main_layout", children=[
    dcc.Interval(id="time_interval", interval=60000),
    dcc.Interval(id="model_time_interval", interval=1800000),
    html.Div(className="header", children=[
        dmc.Text("Chandigarh AQI", className="header_text"),
        html.Div(className="header_measure", children=[DashIconify(icon="fluent:temperature-16-filled", color="white", width=25), html.P(id="header_temperature")]),
        html.Div(className="header_measure", children=[DashIconify(icon="carbon:humidity", color="white", width=25), html.P(id="header_humidity")]),
        html.Div(className="header_measure", children=[DashIconify(icon="tabler:uv-index", color="white", width=25), html.P(id="header_uv")]),
        html.Div(className="header_measure", children=[DashIconify(icon="mi:wind", color="white", width=25), html.P(id="header_wind")]),
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
                html.Div(className="aqi_line_chart_legend_header", children="Air Quality"),
                html.Div(className="aqi_line_chart_legend_item", children="Good (0-50)", style={"background-color": "#377A07"}),
                html.Div(className="aqi_line_chart_legend_item", children="Satisfactory (51-100)", style={"background-color": "#9ACD32"}),
                html.Div(className="aqi_line_chart_legend_item", children="Moderate (101-200)", style={"background-color": "#FFC300"}),
                html.Div(className="aqi_line_chart_legend_item", children="Poor (201-300)", style={"background-color": "#F58F09"}),
                html.Div(className="aqi_line_chart_legend_item", children="Very Poor (301-400)", style={"background-color": "#C41206"}),
                html.Div(className="aqi_line_chart_legend_item", children="Severe (401-500)", style={"background-color": "#810100"})
            ])
        ])
    ]),
    html.Div(className="aqi_measure_row", children=[
        html.Div(className="aqi_measure", children=[
            html.Div(className="aqi_measure_header", children=[DashIconify(icon="streamline:rain-cloud", color="black", width=25), html.P("PM2.5")]),
            daq.Gauge(className="aqi_measure_gauge", id="PM25_gauge", showCurrentValue=True, value=0, size=170, min=0, max=380, units="ug/m^3",
                color={"gradient": True, "ranges": {"#377A07": [0, 30], "#9ACD32": [30, 60], "#FFC300":[60, 90], "#F58F09": [90, 120], "#C41206":[120, 250], "#810100": [250, 380]}}
            )
        ]),
        html.Div(className="aqi_measure", children=[
            html.Div(className="aqi_measure_header", children=[DashIconify(icon="streamline:rain-cloud-solid", color="black", width=25), html.P("PM10")]),
            daq.Gauge(className="aqi_measure_gauge", id="PM10_gauge", showCurrentValue=True, value=0, size=170, min=0, max=700, units="ug/m^3",
                color={"gradient": True, "ranges": {"#377A07": [0, 50], "#9ACD32": [50, 100], "#FFC300":[100, 250], "#F58F09": [250, 350], "#C41206":[350, 430], "#810100": [430, 700]}}
            )
        ]),
        html.Div(className="aqi_measure", children=[
            html.Div(className="aqi_measure_header", children=[html.Img(src="https://jassi-images.s3.ap-southeast-2.amazonaws.com/Sulfur_Dioxide.png", alt="so2", width="30%"), html.P("Sulfur Dioxide")]),
            daq.Gauge(className="aqi_measure_gauge", id="so2_gauge", showCurrentValue=True, value=0, size=170, min=0, max=2000, units="ppm",
                color={"gradient": True, "ranges": {"#377A07": [0, 40], "#9ACD32": [40, 80], "#FFC300":[80, 380], "#F58F09": [380, 800], "#C41206":[800, 1600], "#810100": [1600, 2000]}}
            )
        ]),
        html.Div(className="aqi_measure", children=[
            html.Div(className="aqi_measure_header", children=[html.Img(src="https://jassi-images.s3.ap-southeast-2.amazonaws.com/Carbon_Monoxide.png", alt="co", width="30%"), html.P("Carbon Monoxide")]),
            daq.Gauge(className="aqi_measure_gauge", id="co_gauge", showCurrentValue=True, value=0, size=170, min=0, max=500, units="ug/m^3",
                color={"gradient": True, "ranges": {"#377A07": [0, 5], "#9ACD32": [5, 10], "#FFC300":[10, 13], "#F58F09": [13, 16], "#C41206":[16, 31], "#810100": [31, 500]}}
            )
        ]),
        html.Div(className="aqi_measure", children=[
            html.Div(className="aqi_measure_header", children=[html.Img(src="https://jassi-images.s3.ap-southeast-2.amazonaws.com/ozone.png", alt="o3", width="15%"), html.P("Ozone")]),
            daq.Gauge(className="aqi_measure_gauge", id="o3_gauge", showCurrentValue=True, value=0, size=170, min=0, max=450, units="ppm",
                color={"gradient": True, "ranges": {"#377A07": [0, 26], "#9ACD32": [26, 51], "#FFC300":[51, 87], "#F58F09": [87, 107], "#C41206":[107, 382], "#810100": [382, 450]}}
            )
        ]),
        html.Div(className="aqi_measure", children=[
            html.Div(className="aqi_measure_header", children=[html.Img(src="https://jassi-images.s3.ap-southeast-2.amazonaws.com/Nitrogen_Dioxide.png", alt="no2", width="30%"), html.P("Nitrogen Dioxide")]),
            daq.Gauge(className="aqi_measure_gauge", id="no2_gauge", showCurrentValue=True, value=0, size=170, min=0, max=750, units="ppm",
                color={"gradient": True, "ranges": {"#377A07": [0, 22], "#9ACD32": [22, 44], "#FFC300":[44, 97], "#F58F09": [97, 150], "#C41206":[150, 214], "#810100": [214, 750]}}
            )
        ])
    ])
])


# Updating AQI Line Chart
@app.callback(
        Output("aqi_line_chart", "figure"),
        Input("time_interval", "n_intervals")
)
def update_aqi_line_chart(time_interval):
    df = get_data()
    df = df.rename(columns={"aqi_us_count": "AQI_US", "aqi_in_count": "AQI_IN"})

    aqi_chart = px.line(df, x="time_received", y=["AQI_US", "AQI_IN"], template="plotly_white")
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
@app.callback(
        [Output("header_date", "children"), Output("aqi_reading_count_actual", "children"), Output("PM25_gauge", "value"), Output("PM10_gauge", "value"),
         Output("so2_gauge", "value"), Output("co_gauge", "value"), Output("o3_gauge", "value"), Output("no2_gauge", "value"),
         Output("header_temperature", "children"), Output("header_humidity", "children"), Output("header_uv", "children"), Output("header_wind", "children")],
        Input("time_interval", "n_intervals")
        )
def update_aqi_measures(time_interval):
    df = get_data()
    aqi_us_count = statistics.mean(df["aqi_us_count"])
    aqi_in_count = statistics.mean(df["aqi_in_count"])
    aqi = round(statistics.mean([aqi_us_count, aqi_in_count]))

    measures = df.iloc[0]
    time_received = "Last Update:\n" + measures["time_received"].strftime("%d %B %Y, %I:%M %p")
    temperature = str(measures["temperature"]) + " °C"
    humidity = str(measures["humidity"]) + "%"
    wind = str(measures["wind"]) + " km/hr"

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

    return time_received, aqi, measures["pm25"], measures["pm10"], measures["so2"], measures["co"], measures["o3"], measures["no2"], temperature, humidity, uv, wind

# Updating AQI Predicted Value
@app.callback(
        Output("aqi_reading_count_predicted", "children"),
        Input("model_time_interval", "n_intervals")
  )
def update_aqi_predicted_count(time_interval):
    upcoming_hour = (datetime.now().astimezone(pytz.timezone("Asia/Kolkata"))+timedelta(hours=1)).hour
    # aqi = model.predict(pd.DataFrame({"Hour": [upcoming_hour]}))[0]
    # aqi = round(aqi)
    # return aqi
    return 100

# Running Main App
if __name__ == "__main__":
    app.run_server(debug=False, host="0.0.0.0", port=8004)
