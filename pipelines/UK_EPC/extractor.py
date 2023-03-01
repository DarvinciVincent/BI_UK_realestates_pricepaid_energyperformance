# Import pipeline's configs
from .config import *

# Import custom modules
from libraries import helper_transformation_functions as helper_transform

# Import py libs
import pandas as pd
import numpy as np
import logging
import requests
from sqlalchemy import create_engine
import mysql

# Define global variables
OUTPUT_TABLE_NAME = "residential_building_epc_certificate"
PIPELINE_INPUT = INPUT_DATA["SOURCE"][OUTPUT_TABLE_NAME]
PIPELINE_OUTPUT = OUTPUT_DATA["MYSQL"][OUTPUT_TABLE_NAME]


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
    
    if OUTPUT_TABLE_NAME == "residential_building_epc_certificate":
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
        from_year = current_year
        to_year = current_year
        min_month = current_month
        max_month = current_month
        logging.info(f"Started processing for current month {current_month}-{current_year}.")
    else:
        if (from_year > to_year) or (to_year > current_year) or (from_year < EPC_MIN_YEAR):
            raise ValueError("Invalid year range.")
        min_month = None
        max_month = None
        logging.info(f"Started processing for {from_year} to {to_year}.")

    for year in range(from_year, to_year + 1):
        epc_y = pd.DataFrame(columns=list(PIPELINE_OUTPUT["column_types"].keys()))
        if min_month is None or max_month is None:
        # Define month range for specific year
            if year < current_year:
                max_month = 12
                if year == EPC_MIN_YEAR:
                    min_month = EPC_MIN_MONTH
                else:
                    min_month = 1
            else:
                max_month = current_month
                min_month = 1

        # Extract: API requests for monthly data
        for month in range(min_month, max_month + 1):
            # Extract: API requests
            epc_y_m = get_epc_from_api_by_year_month(from_year=year, from_month=month, to_year=year, to_month=month)
            
            # Transform: Normalization
            if not epc_y_m.empty:
                epc_y_m_transformed = normalize_raw_epc(epc_y_m)
            
                # Concatenate yearly data
                epc_y = pd.concat([epc_y, epc_y_m_transformed], ignore_index=True)
            else:
                logging.warning(f"No data for {month}-{year}")
            logging.info(f"Processing done for {month}-{year}")
        logging.info(f"Processing done for {year}")
        epc_y.drop_duplicates(inplace=True)
        print(epc_y.shape)
        
        # Load: Load to MySQL database
        #TODO
        mydb = create_engine('mysql+mysqlconnector://' + PIPELINE_OUTPUT["username"] + ':' + PIPELINE_OUTPUT["password"] + '@' + PIPELINE_OUTPUT["host"] + ':' + str(PIPELINE_OUTPUT["port"]) + '/' + PIPELINE_OUTPUT["database"] , echo=False)
        epc_y.to_sql(name=PIPELINE_OUTPUT["table_name"], con=mydb, if_exists = 'append', index=False)

    if from_year == to_year:
        logging.info(f"""Monthly update done for {from_year}.""")
    else:
        logging.info(f"""Monthly update done for {from_year}-{to_year}.""")

def main():
    logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S %Z')
    
    ETL_extract_epc(2023, 2023)
    

if __name__ == "__main__":
    main()