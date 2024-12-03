# -*- coding: utf-8 -*-
"""
Created on Mon Dec  2 23:08:06 2024

@author: sulli
"""

from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd
import yaml
from sqlalchemy import create_engine


#Get information from the config
with open('config.yaml','r') as file:
    config = yaml.safe_load(file)

#If the user has created credentials.json
if 'mysql_credentials' in config.keys():
    print('Used credentials from config')
    credentials = config['mysql_credentials']
    
#Otherwise ask for input
else:
    print('Consider creating credentgials.json with relevant information')
    print("Input relevant information for MySQL server")
    credentials = {}
    
    #Asking user for credentials
    credentials['username'] = str(input('Username:'))
    credentials['password'] = str(input("Password:"))
    credentials['host'] = str(input('Host:'))

engine = create_engine(f'mysql+mysqlconnector://{credentials["username"]}'
                       f':{credentials["password"]}@{credentials["host"]}'
                       f':3306/steam_db')
#Creating our connection

df = pd.read_sql("SELECT * FROM player_count_by_game;",engine)


app = Dash()

app.layout = html.Div([
    html.Label(
        'Select Date and Time Range:',
        style={'fontSize': '18px', 'marginBottom': '10px'}
    ),
    html.Div(
        id='slider-output',  # Display the human-readable range here
        style={'marginTop': '20px', 'fontSize': '16px'}
    ),
    html.Div(
        dcc.RangeSlider(
            id='datetime_RangeSlider',
            min=df['timestamp'].min().timestamp(),
            max=df['timestamp'].max().timestamp(),
            value=[
                df['timestamp'].min().timestamp(),
                df['timestamp'].max().timestamp()
            ],
            marks={
                int(t.timestamp()): t.strftime('%d:%m:%Y %H:%M')
                for t in pd.date_range(
                    df['timestamp'].min(),
                    df['timestamp'].max(),
                    freq='D'  # Adjust frequency as needed
                )
            },
            tooltip={"placement": "bottom", "always_visible": True}
        ),
        style={
            'margin': '20px',
            'padding': '10px'
        }
    ),
    
    dcc.Graph(id='player-count')
])


# Callback to update the displayed range
@callback(
    Output('slider-output', 'children'),
    Input('datetime_RangeSlider', 'value')
)
def update_slider_output(value):
    if value:
        start, end = [pd.to_datetime(ts, unit='s') for ts in value]
        return f"Selected Range: {start.strftime('%d:%m:%Y %H:%M')} - {end.strftime('%d:%m:%Y %H:%M')}"
    return "Select a range to view details."



@callback(
    Output('player-count', 'figure'),
    Input('datetime_RangeSlider', 'value')
)
def update_player_count_graph(datetime_range):
    # Convert timestamps back to datetime
    start, end = [pd.to_datetime(ts, unit='s') for ts in datetime_range]

    # Use parameterized query to prevent SQL injection
    query = """
        SELECT timestamp, SUM(count) AS player_count
        FROM player_count_by_game
        WHERE timestamp >= %(start)s AND timestamp <= %(end)s
        GROUP BY timestamp;
    """
    # Execute query with parameters
    df = pd.read_sql(query, engine, params={'start': start, 'end': end})

    # Create a Plotly line chart
    fig = px.line(df, x='timestamp', y='player_count', title='Player Count Over Time')
    
    return fig


if __name__ == '__main__':
    app.run(debug=True)