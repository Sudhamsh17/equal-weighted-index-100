import pandas as pd
import yfinance as yf
from database import MarketDataConn
from utils import get_logger
from joblib import Parallel, delayed
from tenacity import retry, wait_random, stop_after_attempt


class ComputeIndex100:
    """
    Class for computing and storing notional value
    of equal-weighted top 100 stocks by market cap
    """


    def __init__(self, date: str, fetch_quarterly_results: bool = True, show_progress: bool = True) -> None:
        """
        Args:
            date (str): date in "YYYY-MM-DD" format
            fetch_quarterly_results (bool, optional): To fetch the quarterly results
            to update the outstanding shares. Defaults to True.
            show_progress (bool, optional): If enabled shows progress for yfinance downloads
            yfinance downloads. Defaults to True.
        """
        self.db_obj = MarketDataConn()
        self.date = date
        self.fetch_quarterly_results = fetch_quarterly_results
        self.show_progress = show_progress

        self.logger = get_logger(__name__)


    def is_trading_day(self, ticker: str = "AAPL") -> bool:
        """
        Checks if the given date is a valid trading day (i.e., market was open).
        Returns True if valid, False if holiday.

        Args:
            ticker (str, optional): stock symbol. Defaults to "AAPL".

        Returns:
            bool: Returns False if holiday else True
        """
        end_date = pd.to_datetime(self.date) + pd.Timedelta(days=1)
        data = yf.download(ticker, start=self.date, end=end_date, progress=False)

        return self.date in data.index


    def fetch_sp500_stocks(self) -> list[str]:
        """
        Fetches the list of S&P 500 stock tickers

        Returns:
            list[str]: list of S&P 500 stock ticker symbols
        """
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        sp500_df = pd.read_html(url, header=0)[0] # First table contains tickers
        tickers = sp500_df["Symbol"].tolist()

        # To make it consistent to be used in yfinance
        tickers = [symbol.replace('.', '-') for symbol in tickers]

        return tickers


    def get_previous_trading_date(self, ticker: str = 'AAPL') -> str:
        """
        Fetches the most recent trading date before the run date using yfinance.

        Args:
            ticker (str, optional): stock symbol. Defaults to 'AAPL'.

        Raises:
            Exception: If not able to fetch the previous trading date

        Returns:
            str: previous trading date in "YYYY-MM-DD" format
        """
        stock = yf.Ticker(ticker)

        # Just to take some historical set of dates (=10 here) to search for the previous trading date
        start_date = pd.to_datetime(self.date) - pd.Timedelta(days=10)
        history = stock.history(start=start_date, end=self.date)  # Fetch past data

        if not history.empty:
            last_trading_date = history.index[-1].strftime("%Y-%m-%d")  # Get latest available date
            return last_trading_date
        else:
            raise Exception(f"Unable to fetch the previous trading date = {self.date = }")


    @staticmethod
    def fetch_quarterly_shares(ticker: str) -> list[tuple]:
        """
        Fetches past quarters info of outstanding shares for a input stock symbol

        Args:
            ticker (str): stock symbol

        Returns:
            list[tuple]: tuples list consisting of outstanding shares and report date info
        """
        stock = yf.Ticker(ticker)
        balance_sheet = stock.quarterly_balance_sheet.T

        if "Ordinary Shares Number" in balance_sheet.columns:
            shares_data = balance_sheet["Ordinary Shares Number"].dropna()
            return [(ticker, dt_obj.strftime("%Y-%m-%d"), shares) for dt_obj, shares in shares_data.items()]
        else:
            print(f"Unable to fetch data for outstanding shares for {ticker = }")
            return []


    def fetch_and_store_all_quarterly_shares(self, tickers: list[str]) -> None:
        """
        Fetches and stores outstanding shares for all S&P 500 stocks.
        Uses joblib to fetch the information faster

        Args:
            tickers (list[str]): list of tickers for which data will be fetched and stored in DB
        """
        concurrency = 5
        self.logger.info(f"Fetching quarterly shares for {len(tickers)} tickers with {concurrency = }...")
        ticker_data_list = Parallel(n_jobs=concurrency)(delayed(ComputeIndex100.fetch_quarterly_shares)(ticker) for ticker in tickers)

        # Flattening ticker data list
        ticker_data_list = [entry for ticker_data in ticker_data_list for entry in ticker_data]
        self.logger.info(f"Dumping the quarterly shares data to DB...")
        if ticker_data_list:
            self.db_obj.store_quarterly_shares(ticker_data_list)
        self.logger.info("Completed dumping quarterly shares data to DB!")


    @retry(wait=wait_random(30, 90), stop=stop_after_attempt(10))
    def _yf_download_helper(self, tickers: list[str], start_date: str, end_date: str) -> pd.DataFrame:
        """
        Yahoo finance download helper with added retries

        Args:
            tickers (list[str]): list of tickers for which data will be fetched
            start_date (str): download start date in "YYYY-MM-DD" format (inclusive)
            end_date (str): download end date in "YYYY-MM-DD" format (exclusive)

        Raises:
            Exception: Catches Rate limiting exception for retries

        Returns:
            pd.DataFrame: tickers dataframe with OHLCV information
        """
        prices_df = yf.download(tickers, start=start_date, end=end_date, progress=self.show_progress)

        if len(yf.shared._ERRORS):
            self.logger.info("Rate limiting occurred, retrying...")
            raise Exception("Rate limiting retries exceeded while downloading, Try increasing num retries!")

        return prices_df


    def yf_downloader(self, tickers: list[str], start_date: str, end_date: str) -> pd.DataFrame:
        """
        Downloads OHLCV data for input tickers list between start_date and end_date.
        Fetches the data chunk wise to reduce retries because of rate limiting

        Args:
            tickers (list[str]): list of tickers for which data will be fetched
            start_date (str): download start date in "YYYY-MM-DD" format (inclusive)
            end_date (str): download end date in "YYYY-MM-DD" format (exclusive)

        Returns:
            pd.DataFrame: tickers dataframe with OHLCV information
        """
        max_chunk_len = 20
        sub_tickers_list = [tickers[i:i+max_chunk_len] for i in range(0, len(tickers), max_chunk_len)]

        prices_df_list = []
        for ticker_list in sub_tickers_list:
            prices_df_list.append(self._yf_download_helper(ticker_list, start_date, end_date))

        return pd.concat(prices_df_list, axis=1)


    def fetch_closing_prices(self, tickers: list[str]) -> dict:
        """
        Fetches the ticker to closing prices mapping

        Args:
            tickers (list[str]): list of tickers for which data will be fetched

        Returns:
            dict: ticker -> closing_price
        """
        start_date = pd.to_datetime(self.date)
        end_date = pd.to_datetime(self.date) + pd.Timedelta(days=1)

        prices = self.yf_downloader(tickers, start_date, end_date)

        if self.date not in prices.index:
            self.logger.warning(f"Unable to fetch the closing prices for date = {self.date}")

        return prices.loc[self.date]['Close'].to_dict()


    def fetch_market_data(self, prices: dict, tickers: list[str]) -> list[tuple]:
        """
        Fetches current day's close price for all tickers and computes market cap

        Args:
            prices (dict): ticker to closing price mapping
            tickers (list[str]): list of tickers for which data will be fetched

        Returns:
            list[tuple]: input tickers market data to be dumped to DB
        """
        shares_info = self.db_obj.fetch_outstanding_shares(self.date, tickers)

        market_data = []
        for ticker in tickers:

            if shares_info[ticker] and ticker in prices:
                shares_outstanding = shares_info[ticker][0]
                curr_close = prices[ticker]
                market_cap = curr_close * shares_outstanding
                market_data.append((self.date, ticker, shares_outstanding, curr_close, market_cap))
            else:
                self.logger.warning(f"Missing outstanding shares, price data for {ticker = }, date = {self.date}...")

        return market_data


    def rebalance_index(self, prices: dict, new_top_100: list[str], curr_index_value: float) -> None:
        """
        Rebalances index weights i.e. when top 100 stocks change

        Args:
            prices (dict): ticker -> closing price mapping
            new_top_100 (list[str]): list of new top 100 stocks by market cap
            curr_index_value (float): current index value
        """
        weight = 1/len(new_top_100)
        index_composition = []
        for ticker in new_top_100:
            index_composition.append((self.date, ticker, (weight*curr_index_value)/prices[ticker]))

        self.db_obj.store_new_composition(index_composition)


    def track_composition_changes(self, prices: dict, prev_top_100: list[str],
                                  new_top_100: list[str], curr_index_value: float) -> None:
        """
        Tracks stocks added and removed from the index and
        rebalance composition if required and store in DB.

        Args:
            prices (dict): ticker -> closing price mapping
            prev_top_100 (list[str]): list of most recent top 100 stocks by market cap
            new_top_100 (list[str]): list of current 100 stocks by market cap
            curr_index_value (float): current index value
        """
        removed_stocks = set(prev_top_100) - set(new_top_100)
        added_stocks = set(new_top_100) - set(prev_top_100)

        if removed_stocks:
            self.logger.info(f"Stocks removed from top 100: {sorted(list((removed_stocks)))}...")
            self.logger.info(f"Stocks getting added to top 100: {sorted(list((added_stocks)))}...")

        # Rebalancing whenever composition changes
        if removed_stocks or len(prev_top_100) == 0:
            self.logger.info(f"Rebalancing index composition...")
            self.rebalance_index(prices, new_top_100, curr_index_value)


    def dump_index_value(self, prices: dict, prev_top_100: list[str], new_top_100: list[str]) -> float:
        """
        Computes, dumps and returns index value

        Args:
            prices (dict): ticker -> closing price mapping
            prev_top_100 (list[str]): list of most recent top 100 stocks by market cap
            new_top_100 (list[str]): list of current 100 stocks by market cap

        Returns:
            float: computed index value
        """
        recent_composition = self.db_obj.fetch_recent_composition(self.date) # ticker, ticker_qty

        if recent_composition:
            curr_index_value = 0

            for ticker, ticker_qty in recent_composition:
                curr_index_value += prices[ticker]*ticker_qty
        else:
            curr_index_value = 10000

        # Track composition changes and store in DB if requried
        self.track_composition_changes(prices, prev_top_100, new_top_100, curr_index_value)

        self.db_obj.store_index_value(self.date, curr_index_value)

        return curr_index_value


    def compute_index_value(self) -> None:
        """
        Full pipeline:
        fetch data, compute index, track composition,
        rebalancing if required and store results
        """

        if not self.is_trading_day():
            self.logger.info(f"date = {self.date}, Is not a valid date, most likely "
                             "it could be a holiday, so exiting...")
            return

        sp500_tickers = self.fetch_sp500_stocks()
        self.logger.info(f"Succesfully fetched list of SnP 500 stocks list!")

        # Fetch and store quarterly shares for all tickers (Run once per quarter)
        if self.fetch_quarterly_results:
            self.fetch_and_store_all_quarterly_shares(sp500_tickers)

        prices = self.fetch_closing_prices(sp500_tickers)
        market_data = self.fetch_market_data(prices, sp500_tickers)
        self.logger.info(f"Succesfully fetched market data for SnP 500 stocks list!")
        self.db_obj.store_market_data(market_data)

        prev_top_100 = self.db_obj.get_previous_top_100(self.date)  # Fetch previous day's top 100
        new_top_100 = self.db_obj.get_top_100_stocks(self.date)  # Fetch today's top 100

        # Compute index value using the previous index
        index_value = self.dump_index_value(prices, prev_top_100, new_top_100)
        self.logger.info(f"Index Value computed for date = {self.date} --> {index_value:.2f}")

