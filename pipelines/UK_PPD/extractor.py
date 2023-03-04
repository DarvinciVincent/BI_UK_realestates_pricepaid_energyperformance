# Import pipeline's configs
from .config import *

# Import custom modules
from libraries import db_functions as db_func
from libraries import helper_transformation_functions as helper_transform

# Import py libs
import pandas as pd
import numpy as np
import requests
import io
import logging


# Define global variables
OUTPUT_TABLE_NAME = "price_paid"
PIPELINE_INPUT = INPUT_DATA["SOURCE"][OUTPUT_TABLE_NAME]
PIPELINE_OUTPUT = OUTPUT_DATA["DB"][OUTPUT_TABLE_NAME]


# Pipeline's helper functions
def get_ppd_from_api_by_interval(by_interval="yearly", year=None, part_number=None, verbose=False) -> tuple[pd.DataFrame, int, str]:
    if by_interval not in PIPELINE_INPUT["endpoint_url_types"]: raise ValueError("PPD can only be extracted by " + ", ".join(PIPELINE_INPUT["endpoint_url_types"]))
    
    api_url = PIPELINE_INPUT[f"endpoint_url_{by_interval}"]
    
    if by_interval == "yearly":
        api_url = api_url.replace("<YEAR>", str(year))

    if by_interval == "half_yearly":
        if part_number not in [1, 2]: raise ValueError("PPD can only be extracted by part number 1 or 2")
        api_url = api_url.replace("<YEAR>", str(year))
        api_url = api_url.replace("<PART_NUMBER>", str(part_number))

    if verbose: print(f"Extracting from API URL: {api_url}")
    
    ppd_response = requests.get(api_url)
    ppd = ppd_response.content
    ppd = pd.read_csv(io.StringIO(ppd.decode('utf-8')),header=None)
    
    api_status_code = ppd_response.status_code

    return ppd, api_status_code, api_url

def normalize_raw_ppd(ppd) -> pd.DataFrame:
    # Standardize column names
    ppd.columns = list(PIPELINE_OUTPUT["column_types"].keys())
    
    # Standardize nan
    ppd = helper_transform.standardardize_nan(ppd, NAN_PATTERNS, standard=np.nan)

    # Assign dtype
    ppd = ppd.astype(PIPELINE_OUTPUT["column_types"])
    ppd = helper_transform.standardardize_nan(ppd, NAN_PATTERNS, standard=np.nan)

    # Drop duplicates
    ppd.drop_duplicates(inplace=True)
    
    return ppd

# pipeline particular functions
def ETL_raw_ppd_current_month():
    
    # Get current date
    current_date, current_month, year = helper_transform.get_current_datetime()

    logging.info(f"Started processing for {current_month}-{year}.")

    # Connect to db
    mydb = db_func.connect_to_db(DB_URL)

    # Extract: API requests
    ppd, api_status_code, api_url = get_ppd_from_api_by_interval("current_month")
    
    if api_status_code == 200 and not ppd.empty:
        logging.info(f"""New data has been extracted from source successfully.
                         URL: {api_url}""")
        # Transform: Normalization
        ppd = normalize_raw_ppd(ppd)
        
        # Load: Load to database
        ppd.to_sql(PIPELINE_OUTPUT["table_name"], con= mydb, schema=PIPELINE_OUTPUT["db_schema"], if_exists='append', index=False)
        logging.info(f"Processing done for {current_month}-{year} with {ppd.shape[0]} records.")
        
    else:
        logging.warning(f"No records for {current_month}-{year} or API request failed.")
    
    # Drop duplicates from from database
    drop_duplicates_query = f"""DELETE FROM {PIPELINE_OUTPUT["db_schema"]}.{PIPELINE_OUTPUT["table_name"]} T1
                                USING       {PIPELINE_OUTPUT["db_schema"]}.{PIPELINE_OUTPUT["table_name"]} T2
                                WHERE  T1.ctid < T2.ctid       -- delete the "older" ones
                                    AND  T1.transactionid    = T2.transactionid;       -- list columns that define duplicates
                            """
    db_func.execute_sql_query(drop_duplicates_query, mydb)

    logging.info(f"""Monthly update done for {current_month}-{year}.""")

def ETL_raw_ppd_yearly(from_year: int=None, to_year: int=None):

    # Get current date
    current_date, current_month, current_year = helper_transform.get_current_datetime()

    # Define year range for processing
    if from_year is None and to_year is None:
        from_year = current_year-1
        to_year = current_year-1
    elif from_year is not None and to_year is None:
        to_year = current_year-1
    elif from_year is None and to_year is not None:
        from_year = PPD_MIN_YEAR

    from_year = int(from_year)
    to_year = int(to_year)

    # Check invalid year input
    if from_year > to_year or to_year > current_year-1 or from_year < PPD_MIN_YEAR:
        raise ValueError(f"Invalid year input! Price paid data is available from {PPD_MIN_YEAR} to {current_month}-{current_year}.")
    
    logging.info(f"Started processing for available transactions of {from_year}-{to_year}.")
    
    # Connect to db
    mydb = db_func.connect_to_db(DB_URL)

    for year in range(from_year, to_year+1):
        from_month = 1
        to_month = 12

        # Extract: API requests
        ppd, api_status_code, api_url = get_ppd_from_api_by_interval("yearly", year=year)
        
        if api_status_code == 200 and not ppd.empty:
            logging.info(f"""New data has been extracted from source successfully.
                            URL: {api_url}""")
            # Transform: Normalization
            ppd = normalize_raw_ppd(ppd)
            
            # Load: Load to database
            ppd.to_sql(PIPELINE_OUTPUT["table_name"], con= mydb, schema=PIPELINE_OUTPUT["db_schema"], if_exists='append', index=False)
            logging.info(f"Processing done for {year} with {ppd.shape[0]} records.")
        else:
            logging.warning(f"No records for {year} or API request failed.")
        
        #TODO: drop duplicates from from database
        drop_duplicates_query = f"""DELETE FROM {PIPELINE_OUTPUT['db_schema']}.{PIPELINE_OUTPUT['table_name']} T1
                                    USING       {PIPELINE_OUTPUT['db_schema']}.{PIPELINE_OUTPUT['table_name']} T2
                                    WHERE  T1.ctid < T2.ctid
                                        AND  T1.transactionid    = T2.transactionid;
                                """
        
        db_func.execute_sql_query(drop_duplicates_query, mydb)

        logging.info(f"""Update done for {year}.""")

    # Disconnect from database
    db_func.disconnect_db(mydb)
    logging.info(f"""Yearly update done for {from_year}-{to_year}.""")

def ETL_raw_ppd(interval: str="current_month", from_year: int=None, to_year: int=None):

    if interval == "current_month":
        ETL_raw_ppd_current_month()
    elif interval == "yearly":
        ETL_raw_ppd_yearly(from_year=from_year, to_year=to_year)
    else:
        raise ValueError(f"Invalid interval input! Only 'current_month' and 'yearly' are allowed.")

def main(*args):
    logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S %Z')
    
    ETL_raw_ppd(*args)
    

if __name__ == "__main__":
    main()
    
