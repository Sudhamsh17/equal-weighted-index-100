import dash
from dash import dcc, html, dash_table, Input, Output
from stats_helper import StatsHelper


def create_index_analytics_dashboard():

    stats_obj = StatsHelper()

    available_dates = sorted(stats_obj.fetch_index_performace()['date'].to_list())

    stock_data = {}
    for date in available_dates:
        date_df = stats_obj.fetch_index_composition(date)
        date_df.sort_values('composition_in_index_growth', inplace=True, ascending=False)
        stock_data[date] = date_df

    # Create Dash App
    app = dash.Dash("Index Analytics")

    app.layout = html.Div([
        html.H1("Index Analytics Dashboard",
                style={
                    'textAlign': 'center',
                    'backgroundColor': '#1E3A8A',  # Dark blue header
                    'color': 'white',
                    'padding': '15px',
                    'borderRadius': '10px'
                }),

        # Index Performance Line Chart
        html.H2("Index performance for the past month", style={'textAlign': 'center', 'marginBottom': '10px'}),
        html.Div([
            dcc.Graph(id='index-performance-chart')
        ], style={'width': '80%', 'margin': 'auto', 'marginBottom': '50px'}),

        html.Hr(style={'border': '2px solid #1E3A8A', 'width': '80%', 'margin': 'auto', 'marginBottom': '40px'}),

        html.H2("📊 Index Composition Table (Stock contribution in index growth)", style={'textAlign': 'center', 'marginBottom': '40px'}),

        # Centered Dropdown and Label with spacing
        html.Div([
            html.Label("📅 Select Date:", style={'fontSize': '18px', 'fontWeight': 'bold', 'marginRight': '10px'}),
            dcc.Dropdown(
                id='date-dropdown',
                options=[{'label': date, 'value': date} for date in available_dates],
                value=available_dates[0],  # Default to the first date
                clearable=False,
                style={'width': '200px', 'fontSize': '16px'}
            ),
        ], style={'display': 'flex', 'justifyContent': 'center', 'alignItems': 'center', 'marginBottom': '30px'}),

        html.Div([
            # Interactive DataTable
            dash_table.DataTable(
                id='index-composition-table',
                columns=[{"name": "Symbol", "id": "ticker"}, {"name": "Weight (%)", "id": "composition_in_index_growth"}],
                style_table={'width': '60%', 'margin': 'auto'},
                style_cell={'textAlign': 'center', 'fontSize': '16px'},
                sort_action="native",
                filter_action="native",
                page_size=10,  # Show up to 10 rows per page
                style_header={
                    'backgroundColor': '#1E40AF',  # Darker blue header
                    'color': 'white',
                    'fontWeight': 'bold'
                },
                style_data_conditional=[  # Alternating row colors
                    {'if': {'row_index': 'odd'}, 'backgroundColor': '#E0F2FE'},
                    {'if': {'row_index': 'even'}, 'backgroundColor': '#FFFFFF'}
                ]
            )
        ], style={'marginBottom': '50px'}),

        html.Hr(style={'border': '2px solid #1E3A8A', 'width': '80%', 'margin': 'auto', 'marginBottom': '40px'}),

        # Index Summary Line Chart
        html.H2("Index Summary Metrics", style={'textAlign': 'center', 'marginBottom': '10px'}),
        html.Div([
            dcc.Graph(id='index-summary-chart')
        ], style={'width': '80%', 'margin': 'auto', 'marginBottom': '50px'})
    ])


    # Callback to Update Index Performance Chart
    @app.callback(
        Output('index-performance-chart', 'figure'),
        [Input('date-dropdown', 'value')]
    )
    def update_index_chart(_):

        fig = stats_obj.index_performance_plot()
        return fig


    # Callback to Update Table Based on Selected Date
    @app.callback(
        Output('index-composition-table', 'data'),
        [Input('date-dropdown', 'value')]
    )
    def update_stock_data(selected_date):
        df = stock_data[selected_date]  # Get data for the selected date
        return df.to_dict('records')  # Update table


    # Callback to Update Index summary Chart
    @app.callback(
        Output('index-summary-chart', 'figure'),
        [Input('date-dropdown', 'value')]
    )
    def update_summary_chart(_):

        fig = stats_obj.summary_metrics_plots()
        return fig


    print(f"Open the link to see the index analytics dashboard: http://127.0.0.1:8080/ ...")
    app.run_server(host='127.0.0.1', port=8080, debug=True)


if __name__ == '__main__':

    create_index_analytics_dashboard()

