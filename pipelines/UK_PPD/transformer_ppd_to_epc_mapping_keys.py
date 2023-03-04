# Import pipeline's configs
from .config import *

# Import custom modules
from libraries import db_functions as db_func
from libraries import helper_transformation_functions as helper_transform
from . import link_functions as link_func

# Import py libs
import pandas as pd
import logging
from sqlalchemy.engine import Engine


# Define global variables
OUTPUT_TABLE_NAME = "pricepaid_to_residential_epc"
PIPELINE_INPUT_PPD = INPUT_DATA["DB"]["price_paid"]
PIPELINE_INPUT_EPC = INPUT_DATA["DB"]["residential_energy_performance_certificate"]
PIPELINE_OUTPUT = OUTPUT_DATA["DB"][OUTPUT_TABLE_NAME]


def ETL_ppd_to_rre_epc_update_by_year(mydb: Engine, year: int):
    
    # Extract: Get price paid data of year
    year_ppd_query = f"""SELECT {', '.join(PIPELINE_INPUT_PPD['needed_columns'])} FROM {PIPELINE_INPUT_PPD['db_schema']}.{PIPELINE_INPUT_PPD['table_name']}
                        WHERE EXTRACT(year FROM "dateoftransfer") = {year}::numeric;"""

    year_ppd = pd.read_sql(year_ppd_query, mydb)
    # Normalize epc address fields
    year_ppd = helper_transform.standardardize_nan(year_ppd, NAN_PATTERNS, standard="")
    year_ppd = year_ppd.fillna("")
    logging.info(f"PPD data has been extracted from DB with {year_ppd.shape[0]} records.")

    # Get unique postcode in ppd data
    year_ppd_unique_postcodes = year_ppd['postcode'].unique()

    # Loop through postcode in chunks of 1000
    chunksize = 5000
    year_ppd_unique_postcodes_chunks = [year_ppd_unique_postcodes[i * chunksize:(i + 1) * chunksize] for i in range((len(year_ppd_unique_postcodes) + chunksize - 1) // chunksize )]

    for postcode_chunk in year_ppd_unique_postcodes_chunks:
        postcode_chunk = postcode_chunk.tolist()
        postcode_chunk = ['' if j is None else j for j in postcode_chunk]

        # Extract: Get epc data of year with matching postcode
        epc_query = f"""SELECT {', '.join(PIPELINE_INPUT_EPC['needed_columns'])} FROM {PIPELINE_INPUT_EPC['db_schema']}.{PIPELINE_INPUT_EPC['table_name']}
                            WHERE postcode IN {tuple(postcode_chunk)};"""

        ppd_epc_mapping_keys = pd.DataFrame(columns=list(PIPELINE_OUTPUT["column_types"].keys()))

        # Extract: Get epc data with matching postcode
        epc = pd.read_sql(epc_query, mydb)

        if epc.empty: continue
        logging.info(f"A chunk of EPC data has been extracted from DB with {epc.shape[0]} records for linking.")
        
        # Drop duplicates if exists
        epc.drop_duplicates(inplace=True)
        
        # Normalize epc address fields
        epc = helper_transform.standardardize_nan(epc, NAN_PATTERNS, standard="")
        epc = epc.fillna("")
        epc = link_func.normalize_address_variables(epc)

        # Initialize dataframe for chunked epc link keys
        epc_mapping_keys = pd.DataFrame(columns=list(PIPELINE_OUTPUT["column_types"].keys()))
        
        # STAGE 1 DONE
        logging.info("STAGE 1 linking...")
        year_ppd = year_ppd[~year_ppd['transactionid'].isin(list(epc_mapping_keys['transactionid']))]
        link_keys, nLinks, new_unique_link_transactionids = link_func.match_stage_1(year_ppd, epc)
        link_keys = link_keys[['transactionid', 'lmk_key']]
        epc_mapping_keys = pd.concat([epc_mapping_keys, link_keys], ignore_index=True)
        epc_mapping_keys.drop_duplicates(inplace=True)
        
        # STAGE 2 DONE
        logging.info("STAGE 2 linking...")
        year_ppd = year_ppd[~year_ppd['transactionid'].isin(list(epc_mapping_keys['transactionid']))]
        link_keys, nLinks, new_unique_link_transactionids = link_func.match_stage_2(year_ppd, epc)
        link_keys = link_keys[['transactionid', 'lmk_key']]
        epc_mapping_keys = pd.concat([epc_mapping_keys, link_keys], ignore_index=True)
        epc_mapping_keys.drop_duplicates(inplace=True)
        
        # STAGE 3 ONGOING
        logging.info("STAGE 3 linking...")
        year_ppd = year_ppd[~year_ppd['transactionid'].isin(list(epc_mapping_keys['transactionid']))]
        link_keys, nLinks, new_unique_link_transactionids = link_func.match_stage_3_not_flat(year_ppd, epc)
        link_keys = link_keys[['transactionid', 'lmk_key']]
        epc_mapping_keys = pd.concat([epc_mapping_keys, link_keys], ignore_index=True)
        epc_mapping_keys.drop_duplicates(inplace=True)

        # STAGE 4
        # TODO
        
        # Union all link keys
        ppd_epc_mapping_keys = pd.concat([ppd_epc_mapping_keys, epc_mapping_keys], ignore_index=True)
        ppd_epc_mapping_keys.drop_duplicates(inplace=True)
        
    ppd_epc_mapping_keys = ppd_epc_mapping_keys.astype(PIPELINE_OUTPUT["column_types"])

    # Load: Load dataframe as parquet file to S3
    if ppd_epc_mapping_keys.empty:
        logging.info("No new data to load to s3!")
        return None
    else:
        # Load: Load to database
        ppd_epc_mapping_keys.to_sql(PIPELINE_OUTPUT["table_name"], con= mydb, schema=PIPELINE_OUTPUT["db_schema"], if_exists='append', index=False)
        logging.info(f"Processing done for {year} with {ppd_epc_mapping_keys.shape[0]} records.")


# Pipeline's main functions
def ETL_ppd_epc_mapping_keys(from_year: int=None, to_year: int=None):

    # Connect to db
    mydb = db_func.connect_to_db(DB_URL)

    # Get current date
    current_date, current_month, current_year = helper_transform.get_current_datetime()

    # Define year range for processing
    if from_year is None and to_year is None:
        from_year = current_year
        to_year = current_year
    elif from_year is not None and to_year is None:
        to_year = current_year
    elif from_year is None and to_year is not None:
        from_year = PPD_MIN_YEAR

    from_year = int(from_year)
    to_year = int(to_year)

    # Check invalid year input
    if from_year > to_year or to_year > current_year or from_year < PPD_MIN_YEAR:
        raise ValueError(f"Invalid year input! Price paid data is available from {PPD_MIN_YEAR} to {current_month}-{current_year}.")
    
    logging.info(f"Started processing for available transactions of {from_year}-{to_year}.")
    
    for year in range(from_year, to_year+1):
        ETL_ppd_to_rre_epc_update_by_year(mydb, year)
        logging.info(f"""Update done for {year}.""")

    # Drop duplicates from from database
    drop_duplicates_query = f"""DELETE FROM {PIPELINE_OUTPUT["db_schema"]}.{PIPELINE_OUTPUT["table_name"]} T1
                                USING       {PIPELINE_OUTPUT["db_schema"]}.{PIPELINE_OUTPUT["table_name"]} T2
                                WHERE  T1.ctid < T2.ctid       -- delete the "older" ones
                                    AND  T1.transactionid    = T2.transactionid
                                    AND  T1.lmk_key          = T2.lmk_key;       -- list columns that define duplicates
                            """
    db_func.execute_sql_query(drop_duplicates_query, mydb)

def main(*args):
    logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S %Z')
    
    ETL_ppd_epc_mapping_keys(*args)
    

if __name__ == "__main__":
    main()