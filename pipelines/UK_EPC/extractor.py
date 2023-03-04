# Import pipeline's configs
from .config import *

# Import custom modules
from libraries import db_functions as db_func
from libraries import helper_transformation_functions as helper_transform

# Import py libs
import pandas as pd
import numpy as np
import logging
import requests
from sqlalchemy import create_engine

# Define global variables
OUTPUT_TABLE_NAME = "residential_energy_performance_certificate"
PIPELINE_INPUT = INPUT_DATA["SOURCE"][OUTPUT_TABLE_NAME]
PIPELINE_OUTPUT = OUTPUT_DATA["DB"][OUTPUT_TABLE_NAME]


# Pipeline's helper functions
def get_epc_from_api_by_year_month(from_year: str|int, from_month: str|int, to_year: str|int, to_month: str|int) -> pd.DataFrame:
    # API by local authority as a workaround of pagination
    epc_y_m = pd.DataFrame()
    for local_code in EPC_LOCAL_AUTHORITIES.values():
        df_local = pd.DataFrame()
        to_continue = True
        nRows = 0
        nRowsLocal = 0
        while to_continue and nRowsLocal < PIPELINE_INPUT['api_max_read_total']:
            api_url = f"{PIPELINE_INPUT[f'endpoint_url']}from-year={from_year}&from-month={from_month}&to-year={to_year}&to-month={to_month}&local-authority={local_code}&size={PIPELINE_INPUT['api_max_read_size']}&from={nRowsLocal}"

            try:
                response = requests.get(api_url, headers=PIPELINE_INPUT['endpoint_headers'])
                if response.status_code != 200:
                    logging.error(f"API request failed with status code {response.status_code}")
                response = response.json()
                df_local_tmp = pd.DataFrame(response['rows'])
                df_local_tmp = df_local_tmp[response['column-names']]
                df_local = pd.concat([df_local, df_local_tmp], ignore_index=True)
                df_local.drop_duplicates(inplace=True)
                nRows = len(df_local_tmp)
                to_continue = nRows == PIPELINE_INPUT['api_max_read_size']
                nRowsLocal += nRows
            except ValueError:
                break

        epc_y_m = pd.concat([epc_y_m, df_local], ignore_index=True)
        epc_y_m.drop_duplicates(inplace=True)

    return epc_y_m

def normalize_raw_epc(epc: pd.DataFrame) -> pd.DataFrame:
    # Standardize column names
    epc.columns = epc.columns.str.replace('-', '_', regex=False)
    
    # Standardize nan
    epc = helper_transform.standardardize_nan(epc, NAN_PATTERNS, standard=np.nan)
    epc = helper_transform.standardardize_nan(epc, REDUNDANT_PATTERNS, standard=np.nan)
    
    # Assign dtype
    epc = epc.astype(PIPELINE_OUTPUT["column_types"])
    epc = helper_transform.standardardize_nan(epc, NAN_PATTERNS, standard=np.nan)
    
    if OUTPUT_TABLE_NAME == "residential_energy_performance_certificate":
        # Standardize floor level
        epc['floor_level'] = epc['floor_level'].str.lower()
        epc = helper_transform.standardardize_column(epc, 'floor_level', FLOOR_LEVEL_PATTERNS, isregex=True)
        
        # Standardize tenure
        epc['tenure'] = epc['tenure'].str.lower()
    
    # Drop duplicates
    epc.drop_duplicates(inplace=True)
    
    return epc


# Pipeline's main functions
def ETL_extract_epc(from_year: int=None, to_year: int=None):
    
    # Get current date
    current_date, current_month, current_year = helper_transform.get_current_datetime()
    
    if (from_year is None and to_year is None):
        # Load only current month
        interval = "current_month"
        from_year = current_year
        to_year = current_year
        min_month = current_month
        max_month = current_month
        logging.info(f"Started processing for current month {current_month}-{current_year}.")
    else:
        interval = "yearly"
        from_year = int(from_year)
        to_year = int(to_year)

        if (from_year > to_year) or (to_year > current_year) or (from_year < EPC_MIN_YEAR):
            raise ValueError("Invalid year range.")
        
        logging.info(f"Started processing for {from_year} to {to_year}.")

    # Connect to db
    mydb = db_func.connect_to_db(DB_URL)

    for year in range(from_year, to_year + 1):

        if interval == "yearly":
            # Define month range for specific year
            if EPC_MIN_YEAR < year < current_year:
                min_month = 1
                max_month = 12
            elif year == EPC_MIN_YEAR:
                min_month = EPC_MIN_MONTH
                max_month = 12   
            elif year == current_year:
                min_month = 1
                max_month = current_month
            else:
                logging.error(f"Invalid year range for {year}!")
            
            logging.info(f"Started processing for {min_month}-{year} to {max_month}-{year}.")

        # Extract: API requests for monthly data
        for month in range(min_month, max_month + 1):
            # Extract: API requests
            epc_y_m = get_epc_from_api_by_year_month(from_year=year, from_month=month, to_year=year, to_month=month)
            
            # Transform: Normalization
            if not epc_y_m.empty:
                epc_y_m = normalize_raw_epc(epc_y_m)

                # Load: Load to database
                epc_y_m.to_sql(PIPELINE_OUTPUT["table_name"], con= mydb, schema=PIPELINE_OUTPUT["db_schema"], if_exists='append', index=False)
                logging.info(f"Processing done for {month}-{year} with {epc_y_m.shape[0]} records.")
            else:
                logging.warning(f"No data for {month}-{year}")        

        #TODO: drop duplicates from from database
        drop_duplicates_query = f"""DELETE FROM {PIPELINE_OUTPUT["db_schema"]}.{PIPELINE_OUTPUT["table_name"]} T1
                                    USING       {PIPELINE_OUTPUT["db_schema"]}.{PIPELINE_OUTPUT["table_name"]} T2
                                    WHERE  T1.ctid < T2.ctid       -- delete the "older" ones
                                        AND  T1.lmk_key    = T2.lmk_key       -- list columns that define duplicates
                                """
        db_func.execute_sql_query(drop_duplicates_query, mydb)

        logging.info(f"""Update done for {year}.""")

    # Disconnect from database
    db_func.disconnect_db(mydb)

    if interval == "current_month":
        logging.info(f"""Monthly update done for {year}.""")
    elif interval == "yearly":
        logging.info(f"""Yearly update done for {from_year}-{to_year}.""")

def main(*args):
    logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S %Z')
    
    ETL_extract_epc(*args)
    

if __name__ == "__main__":
    main()