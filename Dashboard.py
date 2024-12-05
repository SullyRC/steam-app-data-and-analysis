# -*- coding: utf-8 -*-
"""
Created on Mon Dec  2 23:08:06 2024

@author: sulli
"""

from dash import Dash, html, dcc, callback, Output, Input, State
import plotly.express as px
import pandas as pd
import yaml
from sqlalchemy import create_engine

# Get information from the config
with open('config.yaml','r') as file:
    config = yaml.safe_load(file)

# If the user has created credentials.json
if 'mysql_credentials' in config.keys():
    print('Used credentials from config')
    credentials = config['mysql_credentials']
    
# Otherwise ask for input
else:
    print('Consider creating credentials.json with relevant information')
    print("Input relevant information for MySQL server")
    credentials = {}
    
    # Asking user for credentials
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

# Create map for id and name
def create_map_id_name():
    query = """
        SELECT app_id, name
        FROM game_info;
    """
    result = pd.read_sql(query, engine)
    return dict(zip(result['app_id'], result['name']))

map_id_name = create_map_id_name()

# Initialize app
app = Dash()

# Function to fetch new data
def fetch_new_data(valid_apps=None):
    if valid_apps:
        valid_apps_str = ", ".join(f"'{app}'" for app in valid_apps)
        where_clause = f"WHERE app_id IN ({valid_apps_str})"
    else: 
        where_clause = ""
    query = f"""
    SELECT timestamp, SUM(count) AS count
    FROM player_count_by_game
    {where_clause}
    GROUP BY timestamp
    ORDER BY timestamp DESC;
    """
    new_data = pd.read_sql(query, engine)
    new_data['timestamp'] = pd.to_datetime(new_data['timestamp'])
    return new_data

# Function to fetch treemap data
def fetch_treemap_data(start, end, valid_apps):
    # Format our valid apps
    valid_apps_str = ", ".join(f"'{app}'" for app in valid_apps)
    query = f"""
        SELECT name, SUM(count) AS count 
        FROM player_count_by_game
        WHERE timestamp >= '{start.strftime('%Y-%m-%d %H:%M:%S')}'
        AND timestamp <= '{end.strftime('%Y-%m-%d %H:%M:%S')}'
        AND app_id IN ({valid_apps_str})
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
    if treemap_df.empty:
        return px.treemap(
            pd.DataFrame(columns=['name', 'count']),
            path=['name'],
            values='count',
            title='Player Distribution by Game',
        )
    
    custom_colorscale = [
        [0, 'grey'],  # Minimum hue more grey
        [1, 'blue']   # Maximum hue blue
    ]

    # Create the treemap
    fig = px.treemap(
        treemap_df,
        path=['name'],  # Treemap hierarchy (game names)
        values='count',  # Size of areas based on player count
        title='Player Distribution by Game',
        color='count',  # Color based on player count
        color_continuous_scale=custom_colorscale,  # Custom color scale
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
        title_x=0.5,  # Center-align the title
        height=800  # Increase the height of the plot
    )
    
    return fig

# Function for returning apps that meet a certain condition
def return_valid_apps(selected_features: dict = {},
                      range_features: dict = {}) -> list:
    # Base where clause which is always true
    where_clause = "WHERE 1=1"
    
    # Add additional selections to where if column in list
    for key, values in selected_features.items():
        values_str = ", ".join(f"'{val}'" for val in values)
        where_clause += f" AND {key} IN ({values_str})"
        
    # Add additional selections to where based on range of value
    for key, value_range in range_features.items():
        where_clause += f" AND {key} BETWEEN {value_range[0]} AND {value_range[1]}"
        
    query = f"""
        SELECT DISTINCT(app_id) AS valid_apps
        FROM complete_game_info
        {where_clause}
    """
    return pd.read_sql(query, engine)['valid_apps'].tolist()

# Create a bubble plot for tag data
def create_bubble_plot(tag_df):
    if tag_df.empty:
        return px.scatter(
            pd.DataFrame(columns=['tag', 'tag_count']),
            x='tag', y='tag_count', size='tag_count', title='Game Tags by Count of Games'
        )

    tag_df = tag_df.sort_values(by='tag_count', ascending=False)  # Sort by count of games

    custom_colorscale = [
        [0, 'grey'],  # Minimum hue more grey
        [1, 'blue']   # Maximum hue blue
    ]

    fig = px.scatter(
        tag_df,
        x='tag',  # Game tags on the x-axis
        y='tag_count',  # Count of games on the y-axis
        size='tag_count',  # Size of bubbles based on count of games
        color='tag_count',  # Color based on count of games
        color_continuous_scale=custom_colorscale,  # Custom color scale
        title='Game Tags by Count of Games',
        labels={'tag': 'Game Tags', 'tag_count': 'Count of Games'},
    )

    fig.update_layout(
        title_x=0.5,
        xaxis_title='Tags',
        yaxis_title='Count of Games',
        margin=dict(t=50, l=50, r=50, b=50),
        showlegend=False,
        height=800  # Make the y-axis taller
    )

    fig.update_xaxes(
        categoryorder='total descending'  # Sort x-axis by count of games
    )
    
    return fig

# Fetch tag data for bubble chart
def fetch_tag_data():
    query = """
    SELECT tag.tag, COUNT(*) as tag_count
    FROM tag
    INNER JOIN game_tag
    ON tag.tag_id = game_tag.tag_id
    GROUP BY tag;
    """
    tag_df = pd.read_sql(query, engine)
    return tag_df

app.layout = html.Div([
    html.Div([
        html.Label('Select Date and Time Range:', style={'fontSize': '18px'}),
        dcc.RangeSlider(
            id='datetime_RangeSlider',
            step=10 * 60,  # 10-minute interval
            marks=None,
            tooltip={"placement": "bottom"},
        ),
        html.Button('Reset Selected Game', id='reset-button', n_clicks=0, style={'margin': '10px 0'})
    ], style={'margin': '20px'}),
    html.Div(id='slider-output', style={'marginTop': '20px', 'fontSize': '16px'}),
    dcc.Graph(id='player-count'),
    html.Div([
        dcc.Graph(id='treemap-count', style={'width': '49%', 'display': 'inline-block'}),
        dcc.Graph(id='bubble-chart', style={'width': '49%', 'display': 'inline-block'}),
    ]),
    dcc.Interval(id='interval-component', interval=10 * 60 * 1000, n_intervals=0)
])

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
     Output('treemap-count', 'figure'),
     Output('bubble-chart', 'figure')],
    [Input('datetime_RangeSlider', 'value'),
     Input('treemap-count', 'hoverData'),
     Input('reset-button', 'n_clicks')],
    State('reset-button', 'n_clicks_timestamp')
)
def update_content(value, hoverData, n_clicks, reset_timestamp):
    if not value:
        return "Select a range to view details.", px.line(title='Player Count Over Time'), create_treemap(pd.DataFrame()), create_bubble_plot(pd.DataFrame())

    valid_apps = return_valid_apps()
    start, end = [pd.to_datetime(ts, unit='s') for ts in value]
    output_text = f"Selected Range: {start.strftime('%d:%m:%Y %H:%M')} - {end.strftime('%d:%m:%Y %H:%M')}"

    if hoverData and hoverData['points'] and (n_clicks == 0 or n_clicks % 2 == 0):
        selected_game_name = hoverData['points'][0]['label'].split('<br>')[0]
        selected_game_id = [app_id for app_id, name in map_id_name.items() if name == selected_game_name]
        if selected_game_id:
            filtered_df = fetch_new_data(valid_apps=selected_game_id)
            filtered_df = filtered_df[(filtered_df['timestamp'] >= start) & (filtered_df['timestamp'] <= end)]
            fig = px.line(filtered_df, x='timestamp', y='count', title=f'Player Count Over Time for {selected_game_name}')
        else:
            filtered_df = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]
            fig = px.line(filtered_df, x='timestamp', y='count', title='Player Count Over Time')
    else:
        filtered_df = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]
        fig = px.line(filtered_df, x='timestamp', y='count', title='Player Count Over Time')

    fig.update_layout(title_x=0.5)
    
    # Get our player count by game graph
    treemap_df = fetch_treemap_data(start, end, valid_apps)
    tree_fig = create_treemap(treemap_df)
    
    # Bubble chart for tags
    tag_df = fetch_tag_data()
    bubble_chart_fig = create_bubble_plot(tag_df)
    
    return output_text, fig, tree_fig, bubble_chart_fig

if __name__ == '__main__':
    app.run(debug=True)

