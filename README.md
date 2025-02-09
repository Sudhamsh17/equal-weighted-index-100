# Equal-Weighted Index 100

An implementation of an equal-weighted index fund comprising 100 selected stocks. This project provides tools to construct, analyze, and visualize the performance of an equal-weighted index.

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Data Sources](#data-sources)

## Introduction

Traditional market indices often weight stocks based on market capitalization, giving larger companies more influence over the index's performance. In contrast, an equal-weighted index assigns the same weight to each constituent stock, providing a different perspective on market movements. This project focuses on creating and analyzing such an equal-weighted index of 100 stocks.

## Features

- **Index Construction**: Tools to build an equal-weighted index from a list of 100 stocks.
- **Performance Analysis**: Functions to evaluate the index's performance over time.
- **Visualization**: Interactive charts to visualize index performance and individual stock contributions.

## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/Sudhamsh17/equal-weighted-index-100.git
   cd equal-weighted-index-100
   ```

2. **Set Up a Virtual Environment** (optional but recommended):
   ```bash
   python3 -m venv index_venv
   source index_venv/bin/activate
   ```

3. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

1. **Data Collection and Index Calculation**:
   - Ensure you have access to a reliable data source for stock prices (yfinance in our case).
   - Use the provided scripts in the directory to fetch and store historical price data for the selected 100 stocks.
   - Run the `index_computation_wrapper.py` script to compute the equal-weighted index based on the collected data for the past month.
   - The script dumps the index values over time, which can be used for analysis and visualization.
   ```
   python index_computation_wrapper.py
   ```
   - The above computes and dumps index data from "2025-01-02" to "2025-02-07" to "market_data.db"

2. **Dumping data to excel/pdf for any custom analysis**:
   ```
   >>> from stats_helper import StatsHelper
   >>> stats_obj = StatsHelper()

   → To dump in pdf format:

   >>> stats_obj.dump_index_composition("2025-02-07", "pdf", "index_composition_20250207")
   >>> stats_obj.dump_index_performance("pdf", "index_performance")

   → To dump in excel format:

   >>> stats_obj.dump_index_composition("2025-02-07", "excel", "index_composition_20250207")
   >>> stats_obj.dump_index_performance("excel", "index_performance")
   ```


3. **Visualization**:
   - As I already ran the index computations part and stored the relevant data in market_data.db, you can directly run the below to start the dashboard.
   - Utilize the `dashboard.py` script to generate interactive plots of the index performance.
   - The script supports various chart types, including line charts for overall performance and bar charts for individual stock contributions.
   ```
   python dashboard.py
   ```
   - After running the above command go to the link : http://127.0.0.1:8080/ to see the dashboard

## Data Sources

The accuracy and reliability of the index depend on the quality of the data used. Ensure that you are using reputable data sources for stock prices. Some commonly used data providers include:

- [Yahoo Finance](https://finance.yahoo.com/)


