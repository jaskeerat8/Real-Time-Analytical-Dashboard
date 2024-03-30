# Importing Libraries
import os
import json
import boto3
import statistics
import pandas as pd
from sqlalchemy import create_engine
import dash_daq as daq
import plotly.express as px
import dash_mantine_components as dmc
from dash_iconify import DashIconify
from dash import Dash, html, dcc, Input, Output

def get_data():
    # Setting up Boto3 Client
    region_name = "ap-southeast-2"
    secret_name = "rdsMYSQL"
    session = boto3.session.Session(region_name=region_name, aws_access_key_id=os.environ.get("aws_access_key_id"), aws_secret_access_key=os.environ.get("aws_secret_access_key"))
    sm_client = session.client(service_name="secretsmanager")

    # Reading Data from Secrets Manager
    get_secret_value_response = sm_client.get_secret_value(SecretId=secret_name)
    value = json.loads(get_secret_value_response["SecretString"])
    mysql_host = value["endpoint"]
    mysql_user = value["user"]
    mysql_password = value["password"]
    mysql_db = value["database"]

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
    html.Div(className="header", children=[
        dmc.Text("Chandigarh AQI", className="header_text"),
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
                    html.P(className="aqi_reading_heading", children="Next Predicted AQI"),
                    html.P(className="aqi_reading_count_predicted", id="aqi_reading_count_predicted", children=10)
                ])
            ]),
            html.Div(className="aqi_line_chart_legend", children=[
                html.Div(className="aqi_line_chart_legend_header", children="Air Quality"),
                html.Div(className="aqi_line_chart_legend_item", children="Good (0-50)", style={"background-color": "#85ba7d"}),
                html.Div(className="aqi_line_chart_legend_item", children="Satisfactory (51-100)", style={"background-color": "#fade5a"}),
                html.Div(className="aqi_line_chart_legend_item", children="Moderate (101-200)", style={"background-color": "#f17935"}),
                html.Div(className="aqi_line_chart_legend_item", children="Poor (201-300)", style={"background-color": "#d93739"}),
                html.Div(className="aqi_line_chart_legend_item", children="Very Poor (301-400)", style={"background-color": "#85317e"}),
                html.Div(className="aqi_line_chart_legend_item", children="Severe (401-500)", style={"background-color": "#401523"})
            ])
        ])
    ]),
    html.Div(className="aqi_measure_row", children=[
        html.Div(className="aqi_measure", children=[
            html.Div(className="aqi_measure_header", children=[DashIconify(icon="streamline:rain-cloud", color="black", width=25), html.P("PM2.5")]),
            daq.Gauge(className="aqi_measure_gauge", id="PM25_gauge", showCurrentValue=True, value=0, size=170, min=0, max=500,
                color={"gradient": True, "ranges": {"#85ba7d": [0, 30], "#fade5a": [30, 60], "#f17935":[60, 90], "#d93739": [90, 120], "#85317e":[120, 250], "#401523": [250, 500]}}
            )
        ]),
        html.Div(className="aqi_measure", children=[
            html.Div(className="aqi_measure_header", children=[DashIconify(icon="streamline:rain-cloud-solid", color="black", width=25), html.P("PM10")]),
            daq.Gauge(className="aqi_measure_gauge", id="PM10_gauge", showCurrentValue=True, value=0, size=170, min=0, max=500,
                color={"gradient": True, "ranges": {"#85ba7d": [0, 50], "#fade5a": [50, 100], "#f17935":[100, 250], "#d93739": [250, 350], "#85317e":[350, 430], "#401523": [430, 500]}}
            )
        ]),
        html.Div(className="aqi_measure", children=[
            html.Div(className="aqi_measure_header", children=[html.Img(src="https://jassi-images.s3.ap-southeast-2.amazonaws.com/Sulfur_Dioxide.png", alt="so2", width="30%"), html.P("Sulfur Dioxide")]),
            daq.Gauge(className="aqi_measure_gauge", id="so2_gauge", showCurrentValue=True, value=0, size=170, min=0, max=2400,
                color={"gradient": True, "ranges": {"#85ba7d": [0, 40], "#fade5a": [40, 80], "#f17935":[80, 380], "#d93739": [380, 800], "#85317e":[800, 1600], "#401523": [1600, 2400]}}
            )
        ]),
        html.Div(className="aqi_measure", children=[
            html.Div(className="aqi_measure_header", children=[html.Img(src="https://jassi-images.s3.ap-southeast-2.amazonaws.com/Carbon_Monoxide.png", alt="co", width="30%"), html.P("Carbon Monoxide")]),
            daq.Gauge(className="aqi_measure_gauge", id="co_gauge", showCurrentValue=True, value=0, size=170, min=0, max=500,
                color={"gradient": True, "ranges": {"#85ba7d": [0, 1], "#fade5a": [1, 2], "#f17935":[2, 10], "#d93739": [10, 17], "#85317e":[17, 34], "#401523": [34, 500]}}
            )
        ]),
        html.Div(className="aqi_measure", children=[
            html.Div(className="aqi_measure_header", children=[html.Img(src="https://jassi-images.s3.ap-southeast-2.amazonaws.com/ozone.png", alt="o3", width="15%"), html.P("Ozone")]),
            daq.Gauge(className="aqi_measure_gauge", id="o3_gauge", showCurrentValue=True, value=0, size=170, min=0, max=1500,
                color={"gradient": True, "ranges": {"#85ba7d": [0, 50], "#fade5a": [50, 100], "#f17935":[100, 168], "#d93739": [168, 208], "#85317e":[208, 748], "#401523": [748, 1500]}}
            )
        ]),
        html.Div(className="aqi_measure", children=[
            html.Div(className="aqi_measure_header", children=[html.Img(src="https://jassi-images.s3.ap-southeast-2.amazonaws.com/Nitrogen_Dioxide.png", alt="no2", width="30%"), html.P("Nitrogen Dioxide")]),
            daq.Gauge(className="aqi_measure_gauge", id="no2_gauge", showCurrentValue=True, value=0, size=170, min=0, max=600,
                color={"gradient": True, "ranges": {"#85ba7d": [0, 40], "#fade5a": [40, 80], "#f17935":[80, 180], "#d93739": [180, 280], "#85317e":[280, 400], "#401523": [400, 600]}}
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
    [Output("header_date", "children"), Output("aqi_reading_count_actual", "children"), Output("PM25_gauge", "value"), Output("PM10_gauge", "value"), Output("so2_gauge", "value"), Output("co_gauge", "value"), Output("o3_gauge", "value"), Output("no2_gauge", "value")],
    Input("time_interval", "n_intervals")
)
def update_aqi_measures(time_interval):
    df = get_data()
    aqi_us_count = statistics.mean(df["aqi_us_count"])
    aqi_in_count = statistics.mean(df["aqi_in_count"])
    aqi = round(statistics.mean([aqi_us_count, aqi_in_count]))
    measures = df.iloc[0]
    time_received = "Last Update:\n" + measures["time_received"].strftime("%d %B %Y, %I:%M %p")
    return time_received, aqi, measures["pm25"], measures["pm10"], measures["so2"], measures["co"], measures["o3"], measures["no2"]


# Running Main App
if __name__ == "__main__":
    app.run_server(debug=False)
