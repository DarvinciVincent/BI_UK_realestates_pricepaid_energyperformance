# Import pipeline's configs
from .config import *

# Import custom modules
from libraries import db_functions as db_func

# Import py libs
import logging


def ETL_ppd_enriched_with_rre_epc():

    # Connect to db
    mydb = db_func.connect_to_db(DB_URL)

    # Execute sql script to link data
    db_func.execute_sql_from_file("sql/transform_pricepaid_enriched_with_residential_epc.sql", mydb)

    # Disconnect from db
    db_func.disconnect_db(mydb)
    
    logging.info("PPD data has been enriched with EPC data.")


def main():
    logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S %Z')
    
    ETL_ppd_enriched_with_rre_epc()
    

if __name__ == "__main__":
    main()