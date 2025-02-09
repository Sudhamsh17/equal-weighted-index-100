import os
import sqlite3


class MarketDataConn:
    """
    Class for interacting with SQLite3 database
    """

    def __init__(self, database=os.path.join(os.getcwd(), "market_data.db")) -> None:
        self.database = database

        self._create_tables()


    def _connect_to_db(self) -> sqlite3.Connection:
        """
        Connection to database
        """
        conn = sqlite3.connect(self.database)
        return conn


    def _close_db_connection(self, conn) -> None:
        """
        Close database connection
        """
        conn.commit()
        conn.close()


    def _create_tables(self) -> None:
        """
        Creates required tables if not exists
        """
        conn = self._connect_to_db()
        cursor = conn.cursor()

        # Table for quarterly outstanding shares
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS quarterly_shares (
            ticker TEXT,
            report_date TEXT,
            shares_outstanding REAL,
            PRIMARY KEY (ticker, report_date)
        );
        """)

        # Table for historical market caps
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_caps (
            date TEXT,
            ticker TEXT,
            shares_outstanding REAL,
            closing_price REAL,
            market_cap REAL,
            PRIMARY KEY (date, ticker)
        );
        """)

        # Table for historical index_composition
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS index_composition (
            date TEXT,
            ticker TEXT,
            ticker_qty REAL,
            PRIMARY KEY (date, ticker)
        );
        """)

        # Table for historical index_preformance
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS index_performance (
            date TEXT PRIMARY KEY,
            index_value REAL
        );
        """)

        self._close_db_connection(conn)


    def store_quarterly_shares(self, data: list[tuple]) -> None:
        """
        Stores quarterly shares outstanding data in SQLite

        Args:
            data (list[tuple]): quartely results data to be stored
        """
        conn = self._connect_to_db()
        cursor = conn.cursor()

        cursor.executemany("INSERT OR REPLACE INTO quarterly_shares (ticker, report_date, shares_outstanding) VALUES (?, ?, ?)",
                           data)

        self._close_db_connection(conn)


    def store_market_data(self, market_data: list[tuple]) -> None:
        """
        Stores market cap data in the database

        Args:
            market_data (list[tuple]): market data to be stored
        """
        conn = self._connect_to_db()
        cursor = conn.cursor()

        cursor.executemany("INSERT OR REPLACE INTO market_caps (date, ticker, shares_outstanding, closing_price, market_cap) VALUES (?, ?, ?, ?, ?)",
                           market_data)

        self._close_db_connection(conn)


    def get_top_100_stocks(self, date: str) -> list[str]:
        """
        Fetches top 100 stocks by market cap for a given date

        Args:
            date (str): date in "YYYY-MM-DD" format

        Returns:
            list[str]: top 100 list of stock symbols by market cap
        """
        conn = self._connect_to_db()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT ticker FROM market_caps
            WHERE date = ?
            ORDER BY market_cap DESC
            LIMIT 100
        """, (date,))

        top_100 = sorted([row[0] for row in cursor.fetchall()])
        self._close_db_connection(conn)
        return top_100


    def get_previous_top_100(self, date: str) -> list[str]:
        """
        Fetches the top 100 stocks from the last available
        trading day before input date

        Args:
            date (str): date in "YYYY-MM-DD" format

        Returns:
            list[str]: top 100 list of stock symbols from index composition
        """
        conn = self._connect_to_db()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT ticker FROM index_composition
            WHERE date = (SELECT MAX(date) FROM index_composition WHERE date < ?)
        """, (date,))

        results = cursor.fetchall()
        if results:
            prev_top_100 = sorted([row[0] for row in results])
        else:
            prev_top_100 = []
        self._close_db_connection(conn)
        return prev_top_100


    def fetch_outstanding_shares(self, date: str, tickers: list[str]) -> dict:
        """
        Returns dictionary of tickers to outstanding shares

        Args:
            date (str): date in "YYYY-MM-DD" format
            tickers (list[str]): list of tickers to fetch the info

        Returns:
            dict: ticker -> outstanding shares info
        """
        conn = self._connect_to_db()
        cursor = conn.cursor()

        shares_info = {}
        for ticker in tickers:
            cursor.execute("""
                SELECT shares_outstanding FROM quarterly_shares
                WHERE ticker = ? AND report_date <= ? ORDER BY report_date DESC LIMIT 1
            """, (ticker,date))
            shares_info[ticker] = cursor.fetchone()

        self._close_db_connection(conn)

        return shares_info


    def store_new_composition(self, data: list[tuple]) -> None:
        """
        Stores new composition data into index_composition table

        Args:
            data (list[tuple]): new composition data to be stored
        """
        conn = self._connect_to_db()
        cursor = conn.cursor()

        cursor.executemany("INSERT OR REPLACE INTO index_composition (date, ticker, ticker_qty) VALUES (?, ?, ?)",
                           data)

        self._close_db_connection(conn)


    def fetch_recent_composition(self, date: str) -> list[tuple]:
        """
        Return most recent composition of Index from index_composition table

        Args:
            date (str): date in "YYYY-MM-DD" format

        Returns:
            list[tuple]: list of ticker, ticker_qty entries
        """
        conn = self._connect_to_db()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT ticker, ticker_qty FROM index_composition
            WHERE date = (SELECT MAX(date) FROM index_composition WHERE date < ?)
        """, (date,))

        results = cursor.fetchall()
        self._close_db_connection(conn)

        return results


    def get_previous_index_value(self, date: str) -> tuple:
        """
        Fetches the most recent index value before the given date

        Args:
            date (str): date in "YYYY-MM-DD" format

        Returns:
            tuple: tuple with index_value
        """
        conn = self._connect_to_db()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT index_value FROM index_performance
            WHERE date < ? ORDER BY date DESC LIMIT 1
        """, (date,))

        result = cursor.fetchone()
        self._close_db_connection(conn)

        return result


    def store_index_value(self, date: str, index_value: float) -> None:
        """
        Stores index value in index_performance table

        Args:
            date (str): date in "YYYY-MM-DD" format
            index_value (float): Index value to be stored
        """
        conn = self._connect_to_db()
        cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO index_performance (date, index_value) VALUES (?, ?)",
                       (date, index_value))
        self._close_db_connection(conn)


    def run_custom_query(self, query: str) -> list[tuple]:
        """
        To run a custom query to fetch the corresponding results

        Args:
            query (str): SQL query

        Returns:
            list[tuple]: results of the query
        """

        conn = self._connect_to_db()
        cursor = conn.cursor()

        cursor.execute(query)

        results = cursor.fetchall()
        self._close_db_connection(conn)

        return results

