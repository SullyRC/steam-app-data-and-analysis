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

# Initial dataframe
def fetch_initial_data():
    query = """
    SELECT timestamp, SUM(count) AS count
    FROM player_count_by_game
    GROUP BY timestamp
    ORDER BY timestamp DESC;
    """
    df = pd.read_sql(query, engine)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

df = fetch_initial_data()

# Initialize app
app = Dash()

app.layout = html.Div([
    html.Label(
        'Select Date and Time Range:',
        style={'fontSize': '18px', 'marginBottom': '10px'}
    ),
    html.Div(
        dcc.RangeSlider(
            id='datetime_RangeSlider',
            tooltip={"placement": "bottom", "always_visible": False},
                     step=10*60, # 10 minute inteval
                     dots=False,
                     marks=None
                     ),
        style={'margin': '20px', 'padding': '10px'}
    ),
    html.Div(
        id='slider-output',
        style={'marginTop': '20px', 'fontSize': '16px'}
    ),
    dcc.Graph(id='player-count'),
    dcc.Graph(id='treemap-count'),
    dcc.Interval(
        id='interval-component',
        interval=10*60*1000,  # 10 minutes in milliseconds
        n_intervals=0
    )
])

# Function to fetch new data
def fetch_new_data():
    query = """
    SELECT timestamp, SUM(count) AS count
    FROM player_count_by_game
    WHERE name LIKE 'Palworld'
    GROUP BY timestamp
    ORDER BY timestamp DESC;
    """
    new_data = pd.read_sql(query, engine)
    new_data['timestamp'] = pd.to_datetime(new_data['timestamp'])
    return new_data

# Function to fetch treemap data
def fetch_treemap_data(start, end):
    query = f"""
        SELECT name, SUM(count) AS count 
        FROM player_count_by_game
        WHERE timestamp >= \'{start.strftime('%Y-%m-%d %H:%M:%S')}\'
        AND timestamp <= \'{end.strftime('%Y-%m-%d %H:%M:%S')}\'
        GROUP BY name;
    """
    treemap_df = pd.read_sql(query, engine)
    
    # For formatting the output
    treemap_df['label'] = treemap_df.apply(
        lambda row: f"{row['name']}<br>{row['count']} players",
        axis=1
    )
    return treemap_df

# Function to create our treemap
def create_treemap(treemap_df):
    
    # Create the treemap
    fig = px.treemap(
        treemap_df,
        path=['name'],  # Treemap hierarchy (game names)
        values='count',  # Size of areas based on player count
        title='Player Distribution by Game',
        color='count',  # Color based on player count
        color_continuous_scale='Blues',
        labels = { row['name']:f"{row['name']}\n{row['count']}" 
                  for (index,row) in treemap_df.iterrows()}
    )
    
    # Add the labels to the treemap
    fig.data[0].textinfo = 'label'  # Show custom labels
    fig.data[0].hovertemplate = (
        "Game: %{label}<br>Total Players: %{value}<extra></extra>"
    )  # Enhance hover tooltips
    
    fig.update_layout(
        margin=dict(t=30, l=0, r=0, b=0),  # Adjust layout margins
        title_x=0.5  # Center-align the title
    )
    
    return fig

@callback(
    [Output('datetime_RangeSlider', 'min'),
     Output('datetime_RangeSlider', 'max'),
     Output('datetime_RangeSlider', 'value')],
    Input('interval-component', 'n_intervals')
)
def update_continuous_slider(n_intervals):
    global df
    df = fetch_new_data()  # Fetch updated data
    min_timestamp = df['timestamp'].min().timestamp()
    max_timestamp = df['timestamp'].max().timestamp()

    # Set slider's min, max, and initial value (full range)
    return min_timestamp, max_timestamp, [min_timestamp, max_timestamp]

@callback(
    [Output('slider-output', 'children'),
     Output('player-count', 'figure'),
     Output('treemap-count','figure')],
    Input('datetime_RangeSlider', 'value')
)
def update_content(value):
    if not value:
        return "Select a range to view details.", px.line(title='Player Count Over Time')

    start, end = [pd.to_datetime(ts, unit='s') for ts in value]
    output_text = f"Selected Range: {start.strftime('%d:%m:%Y %H:%M')} - {end.strftime('%d:%m:%Y %H:%M')}"
    filtered_df = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]

    fig = px.line(filtered_df, x='timestamp', y='count', title='Player Count Over Time')
    fig.update_layout(title_x=0.5)
    
    treemap_df = fetch_treemap_data(start, end)
    
    tree_fig = create_treemap(treemap_df)
    
    return output_text, fig, tree_fig


if __name__ == '__main__':
    
    app.run(debug=True)