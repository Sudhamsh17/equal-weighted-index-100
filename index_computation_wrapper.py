import pandas as pd
from time import sleep
from compute_index100 import ComputeIndex100
from utils import get_logger


if __name__ == "__main__":

    logger = get_logger(__name__)

    start_date = "2025-01-02"
    end_date = "2025-02-07"

    dates_list = []
    for i in range(100):
        curr_date = (pd.to_datetime(start_date) + pd.Timedelta(days=i)).strftime("%Y-%m-%d")

        if curr_date > end_date:
            break
        dates_list.append(curr_date)

    logger.info(f"Index computation will be computed for {len(dates_list)} dates...")

    compute_obj = ComputeIndex100(date=start_date,
                                  fetch_quarterly_results=False,
                                  show_progress=True)

    sleep_time_in_sec = 60
    for cntr, run_date in enumerate(dates_list):

        logger.info(f"\n\n#####   {cntr+1}/{len(dates_list)}   ##### Computing for date : {run_date}")
        compute_obj.date = run_date
        compute_obj.fetch_quarterly_results = (cntr==0)

        compute_obj.compute_index_value()

        if cntr <= (len(dates_list) - 1):
            logger.info(f"Sleeping for {sleep_time_in_sec} sec before computing for next date...")
            sleep(sleep_time_in_sec)

    logger.info(f"Successfully computed for all dates between {start_date}, {end_date}")

