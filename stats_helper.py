import os
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from database import MarketDataConn
from utils import dump_df_to_pdf, get_logger


class StatsHelper:
    """
    Class for fetching Index stats from DB for analysis
    """


    def __init__(self, output_base_dir: str = os.getcwd()) -> None:
        """
        Args:
            output_base_dir (str, optional): Base directory to dump the stats. Defaults to os.getcwd().
        """

        self.db_obj = MarketDataConn()

        self.stats_base_dir = os.path.join(output_base_dir, "stats")
        os.makedirs(self.stats_base_dir, exist_ok=True)

        self.logger = get_logger(__name__)


    def fetch_index_performace(self) -> pd.DataFrame:
        """
        Fetches the data from index_performance table

        Returns:
            pd.DataFrame: index performance df
        """

        query = "SELECT * from index_performance;"
        results = self.db_obj.run_custom_query(query)

        df = pd.DataFrame(results, columns=['date', 'index_value'])

        return df


    def fetch_index_composition_dates(self) -> list[str]:
        """
        Dates of rebalancing index

        Returns:
            list[str]: list of dates in "YYYY-MM-DD" format
        """

        query = "SELECT distinct(date) FROM index_composition;"
        dates_list = sorted([res[0] for res in self.db_obj.run_custom_query(query)])

        return dates_list


    def fetch_index_composition(self, date: str) -> pd.DataFrame:
        """
        Returns index composition for an input date

        Args:
            date (str): date in "YYYY-MM-DD" format

        Returns:
            pd.DataFrame: index composition df
        """

        query = f"""
                SELECT B.date, A.ticker, ROUND(100*(A.ticker_qty*B.closing_price)/(SELECT index_value
                from index_performance WHERE date = '{date}'), 2) from index_composition AS A
                INNER JOIN (SELECT date, ticker, closing_price from market_caps WHERE date = '{date}') AS B
                ON A.ticker = B.ticker
                WHERE A.date = (SELECT MAX(date) FROM index_composition WHERE date <= '{date}');
                """
        results = self.db_obj.run_custom_query(query)

        df = pd.DataFrame(results, columns=['date', 'ticker', 'composition_in_index_growth'])

        return df


    def dump_df_helper(self, df: pd.DataFrame, file_type: str,
                       output_file_name: str, table_name: str) -> None:
        """
        Helper function to dump a dataframe to excel/pdf

        Args:
            df (pd.DataFrame): dataframe to be dumped
            file_type (str): output file type (options: ['excel', 'pdf'])
            output_file_name (str): output file name without extension
            table_name (str): table/sheet name to be displayed in output file

        Raises:
            ValueError: if input df is empty or invalid file_type argument
        """

        if df.empty:
            raise ValueError(f"No {table_name} data found for the given inputs")

        if file_type == 'excel':
            output_file_path = f"{self.stats_base_dir}/{output_file_name}.xlsx"
            write_mode = 'a' if os.path.exists(output_file_path) else 'w'
            with pd.ExcelWriter(output_file_path, mode=write_mode) as writer:
                df.to_excel(writer, sheet_name=table_name, index=False)

        elif file_type == 'pdf':
            dump_df_to_pdf(df, f"{self.stats_base_dir}/{output_file_name}.pdf", table_name)

        else:
            raise ValueError(f"Invalid file_type arg, file_type not in ['pdf', 'excel']")

        self.logger.info(f"Successfully saved {table_name} at {output_file_name}.{file_type}")


    def dump_index_performance(self, file_type: str, output_file_name: str) -> None:
        """
        Dumps index performance to output_file_name with corresponding file_type

        Args:
            file_type (str): output file type (options: ['excel', 'pdf'])
            output_file_name (str): output file name without extension
        """

        df = self.fetch_index_performace()
        self.dump_df_helper(df, file_type, output_file_name, 'Index Performance')


    def dump_index_composition(self, date: str, file_type: str, output_file_name: str) -> None:
        """
        Dumps index composition to output_file_name with corresponding file_type

        Args:
            date (str): date in "YYYY-MM-DD" format
            file_type (str): output file type (options: ['excel', 'pdf'])
            output_file_name (str): output file name without extension
        """

        df = self.fetch_index_composition(date)
        self.dump_df_helper(df, file_type, output_file_name, f'Index Comp. for {date}')


    def _update_fig_properties(self, fig: go.Figure) -> None:
        """
        Updates input plotly Figure object properties in place

        Args:
            fig (go.Figure): plotly Figure object
        """

        fig.update_layout(
            plot_bgcolor='white'
        )
        fig.update_xaxes(
            mirror=True,
            ticks='outside',
            showline=True,
            linecolor='black',
            gridcolor='lightgrey'
        )
        fig.update_yaxes(
            mirror=True,
            ticks='outside',
            showline=True,
            linecolor='black',
            gridcolor='lightgrey',
            zerolinecolor='lightgrey',
            zerolinewidth=1,
            tickformat = "digits"
        )


    def index_performance_plot(self) -> go.Figure:
        """
        Returns plot(Figure object) corresponding to index_performance

        Returns:
            go.Figure: plotly Figure object
        """


        df = self.fetch_index_performace()

        composition_change_dates = self.fetch_index_composition_dates()[1:]
        composition_change_df = df[df['date'].isin(composition_change_dates)].reset_index(drop=True)

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df['date'], y=df['index_value'].round(3), mode='lines', name='index_performance',
                                hovertemplate='<br>date : %{x}<br>index_value : %{y}<br><extra></extra>'))
        fig.add_trace(go.Scatter(x=composition_change_df['date'], y=composition_change_df['index_value'].round(3),
                                mode='markers', name='composition_change_dates',
                                hovertemplate='<br>date : %{x}<br>index_value : %{y}<br><extra></extra>'))
        self._update_fig_properties(fig)

        fig.update_layout(
            margin=dict(l=40, r=40, t=40, b=40),
            xaxis_title = "<b>Date</b>",
            yaxis_title = "<b>Index Value</b>"
        )

        return fig


    def summary_metrics_plots(self) -> go.Figure:
        """
        Returns plot(Figure object) corresponding to index summary metrics

        Returns:
            go.Figure: plotly Figure object
        """

        df = self.fetch_index_performace()

        df['daily_returns'] = (df['index_value'].pct_change()*100).fillna(0).round(3)

        df['cumulative_returns'] = (((df['index_value'] - 10000)/10000) *100).round(3)

        composition_change_dates = self.fetch_index_composition_dates()[1:]
        df['composition_change'] = False
        df.loc[df['date'].isin(composition_change_dates), 'composition_change'] = True
        df['num_composition_changes'] = df['composition_change'].cumsum()

        fig = go.Figure()

        fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.06,
                            subplot_titles=("<b>Daily Returns</b>", "<b>Cumulative Returns</b>",
                                            "<b>Cumulative no.of composition changes</b>"))

        # Add first line plot
        fig.add_trace(go.Scatter(x=df['date'], y=df['daily_returns'],
                                name="daily_returns", mode='lines+markers',
                                hovertemplate='<br>date : %{x}<br>daily_returns : %{y}<br><extra></extra>'), row=1, col=1)

        # Add second line plot
        fig.add_trace(go.Scatter(x=df['date'], y=df['cumulative_returns'],
                                name="cumulative_returns", mode='lines+markers',
                                hovertemplate='<br>date : %{x}<br>cumulative_returns : %{y}<br><extra></extra>'), row=2, col=1)

        # Add third line plot
        fig.add_trace(go.Scatter(x=df['date'], y=df['num_composition_changes'],
                                name="num_composition_changes", mode='lines+markers',
                                hovertemplate='<br>date : %{x}<br>num_composition_changes : %{y}<br><extra></extra>'), row=3, col=1)

        self._update_fig_properties(fig)

        fig.update_yaxes(title_text="<b>returns (%)</b>", row=1, col=1)
        fig.update_yaxes(title_text="<b>returns (%)</b>", row=2, col=1)
        fig.update_yaxes(title_text="<b>num_composition_changes</b>", row=3, col=1)
        fig.update_xaxes(title_text="<b>Date</b>", row=3, col=1)

        fig.update_layout(
            height=1500,
            showlegend=False,
            plot_bgcolor="white",
            margin=dict(l=40, r=40, t=40, b=40)
        )

        return fig


