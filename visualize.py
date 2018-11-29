import dash
from dash.dependencies import Output, Event
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
from datetime import date
from cassandra.cluster import Cluster
import pandas as pd


app = dash.Dash(__name__)
app.layout = html.Div(
    [   #html.H2('ID2221 Project'),
        dcc.Graph(id='live-graph', animate=False),
        dcc.Interval(
            id='graph-update',
            interval=1*1000 # update once every second
        ),
    ]
)

cluster = Cluster()
session = cluster.connect('trend_space')

minutes = 30 # number of minutes back to show

@app.callback(Output('live-graph', 'figure'),
              events=[Event('graph-update', 'interval')])
def update_graph_scatter():
    try: 
        rows = session.execute('SELECT * FROM trends')

        # Create dataframe
        df = pd.DataFrame(columns=['neg','neu', 'pos'])
        for row in rows:
            df.loc[row.datetime]=[row.neg, row.neu, row.pos]

        # Parse datetime
        df.index = pd.to_datetime(df.index.values, format='%Y/%m/%d %H:%M', errors='ignore')

        # Add column total
        # df["total"] = df.negative + df.neutral + df.positive

        # Sort after time, early to late
        df.sort_index(inplace=True)

        # Take x, y from last 10 minutes, update
        X = df.index.strftime("%H:%M").values[-minutes:]
        Y_neg = df.neg.values[-minutes:]
        Y_neu = df.neu.values[-minutes:]
        Y_pos = df.pos.values[-minutes:]

        # Define bars
        trace1 = go.Bar(
            x=X,
            y=Y_neg,
            name='Negative',
            marker=dict(
            color='rgba(219, 64, 82, 0.7)',
            ),
            #opacity=0.6
        )
        trace2 = go.Bar(
            x=X,
            y=Y_neu,
            name='Neutral',
            marker=dict(
            color='rgba(55, 128, 191, 0.7)',
            ),
            #opacity=0.6
        )
        trace3 = go.Bar(
            x=X,
            y=Y_pos,
            name='Positive',
            marker=dict(
            color='rgba(50, 171, 96, 0.7)',
            ),
            #opacity=0.6
        )

        data = [trace1, trace2, trace3]
        layout = go.Layout(
            barmode='stack',
            title='Twitter mentions of Spotify',
            xaxis=dict(
                tickfont=dict(
                    size=12,
                    color='rgb(107, 107, 107)'
                )
            ),
            yaxis=dict(
                title='Number of mentions',
                titlefont=dict(
                    size=14,
                    color='rgb(107, 107, 107)'
                ),
                tickfont=dict(
                    size=12,
                    color='rgb(107, 107, 107)'
                )
            )
        )

        return {'data': data, "layout": layout}

    except Exception as e:
        with open('errors.txt','a') as f:
            f.write(str(e))
            f.write('\n')


if __name__ == '__main__':
    app.run_server(debug=True)