# Import pipeline's configs
from .config import *

# Import custom modules
from libraries import db_functions as db_func
from libraries import helper_transformation_functions as helper_transform

# Import py libs
import pandas as pd
import numpy as np
import logging
import zipfile
from datetime import datetime
import os


# Define global variables
OUTPUT_TABLE_NAME = "unique_property_reference_number"
PIPELINE_INPUT = INPUT_DATA["SOURCE"][OUTPUT_TABLE_NAME]
PIPELINE_OUTPUT = OUTPUT_DATA["DB"][OUTPUT_TABLE_NAME]

# Define helper functions
def normalize_raw_uprn(uprn: pd.DataFrame):
    # Standardize column names
    uprn.columns = list(PIPELINE_OUTPUT["column_types"].keys())
    
    # Standardize nan
    uprn = helper_transform.standardardize_nan(uprn, NAN_PATTERNS, standard=np.nan)
    uprn = uprn.fillna(np.nan)
    
    # Assign dtype
    uprn = uprn.astype(PIPELINE_OUTPUT["column_types"])
    uprn = helper_transform.standardardize_nan(uprn, NAN_PATTERNS, standard=np.nan)
    
    # Drop duplicates
    uprn.drop_duplicates(inplace=True)
    
    return uprn

# Pipeline's main functions
def ETL_uprn_full():

    # Extract: API requests
    url = PIPELINE_INPUT['endpoint_url']
    file_path = PIPELINE_INPUT['save_to_path']
    status = os.system(f"wget {url} -O {file_path}")
    success = True

    uprn = pd.DataFrame()

    if success:
        logging.info(f"Downloading from URL {url} succeeded.")
        with zipfile.ZipFile(file_path) as z:
            file_names = z.namelist()
            logging.info(file_names)
            try:
                uprn_file_name = [file_name for file_name in file_names if "osopenuprn" in file_name][0]
                logging.info(f"Getting data from {uprn_file_name}")
                file_name_prefix = uprn_file_name.split(".")[0]
                update_date_str = file_name_prefix.split("_")[-1]
                update_date_str_format = '%Y%m'
                update_date_datetime = datetime.strptime(update_date_str, update_date_str_format)
                update_year = update_date_datetime.year
                update_month = update_date_datetime.month
                logging.info(f"Newest data was updated on: {update_year}-{update_month:02d}")

                with z.open(uprn_file_name) as f:
                    logging.info(f"Extracting newest data released on {update_year}-{update_month:02d}")
                    uprn = pd.read_csv(f)
            except IndexError:
                logging.error("No file name matches \"osopenuprn\"")
        
        if uprn.empty:
            logging.warning("No data extracted from the zip file. No data to be load!")
            return None
        else:
            # Transform: Normalization
            uprn = normalize_raw_uprn(uprn)

            # Load: Load to database
            current_date, current_month, current_year = helper_transform.get_current_datetime()
            
            ### Connect to db
            mydb = db_func.connect_to_db(DB_URL)
            uprn.to_sql(PIPELINE_OUTPUT["table_name"], con= mydb, schema=PIPELINE_OUTPUT["db_schema"], if_exists='replace', index=False, chunksize=5000)
            logging.info(f"Processing done on {current_date} for the newest UPRN data released on  {update_month}-{update_year}.")
        
        os.remove(file_path)
    else:
        logging.error(f"Downloading from URL {url} failed")


def main():
    logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S %Z')
    
    ETL_uprn_full()
    

if __name__ == "__main__":
    main()