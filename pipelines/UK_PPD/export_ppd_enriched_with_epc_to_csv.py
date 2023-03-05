# Import pipeline's configs
from .config import *

# Import custom modules
from libraries import db_functions as db_func
from libraries import helper_transformation_functions as helper_transform

# Import py libs
import logging


# Define global variables
OUTPUT_TABLE_NAME = "pricepaid_enriched_with_residential_epc"
PIPELINE_OUTPUT = OUTPUT_DATA["DB"][OUTPUT_TABLE_NAME]

EXPORT_LOCATION = "/Users/thanh/Documents/KAGGLE/UK_PRICEPAID_RESIDENTIAL_ENERGY_PERFORMANCE_CERTIFICATE/pricepaid_enriched_with_residential_epc"
EXPORT_COMMAND = f"COPY (SELECT * FROM {PIPELINE_OUTPUT['db_schema']}.{PIPELINE_OUTPUT['table_name']} WHERE EXTRACT(year FROM \"dateoftransfer\") = <YEAR>::numeric) TO \'{EXPORT_LOCATION}/{PIPELINE_OUTPUT['table_name']}_<YEAR>.csv\' CSV HEADER;\n"""


def export_yearly_ppd_enriched_with_epc(from_year=None, to_year: int=None):

    # Connect to db
    mydb = db_func.connect_to_db(DB_URL)
    
    # Get current date
    current_date, current_month, current_year = helper_transform.get_current_datetime()

    # Define year range for processing
    if from_year is None and to_year is None:
        from_year = PPD_MIN_YEAR
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
    
    logging.info(f"Started exporting for available transactions of {from_year}-{to_year}.")

    # Modify export command and Export data for each year
    for year in range(from_year, to_year + 1):
        export_command = EXPORT_COMMAND.replace("<YEAR>", str(year))
        db_func.execute_sql_query(export_command, mydb)
        logging.info(f"Exporting done for {year}.")

    # Disconnect from db
    db_func.disconnect_db(mydb)
    
    logging.info("PPD data has been enriched with EPC data.")


def main(*args):
    logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S %Z')
    
    export_yearly_ppd_enriched_with_epc(*args)
    

if __name__ == "__main__":
    main()