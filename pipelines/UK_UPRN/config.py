import os
from dotenv import load_dotenv

# ENVIRONMENT VARIABLES
load_dotenv()

### MYSQL
MYSQL_USERNAME = str(os.getenv("MYSQL_USERNAME"))
MYSQL_PASSWORD = str(os.getenv("MYSQL_PASSWORD"))


# RUNTIME CONFIGURATIONS
COLUMN_TYPES = {"unique_property_reference_number": {"uprn": "string",
                                                     "x_coordinate": float,
                                                     "y_coordinate": float,
                                                     "latitude": float,
                                                     "longitude": float
                                                     }
                }

NAN_PATTERNS = ["N/A", "NaN", "nan", "unknown", "UNKNOWN", "Unknown", "INVALID!", "NODATA!", "NO DATA!", ""]

# OUTPUT_DATA -> connection -> output_table_name -> bucket, country, subject, source, table_name, path_date_params, file_extension, column_types
OUTPUT_DATA =  {"MYSQL": {"unique_property_reference_number": {"table_name": "unique_property_reference_number",
                                                                    "host":  "localhost",  
                                                                    "port": 3306, 
                                                                    "database": "ppd_epc",
                                                                    "username": MYSQL_USERNAME,
                                                                    "password": MYSQL_PASSWORD,
                                                                    "column_types": COLUMN_TYPES["unique_property_reference_number"]
                                                                    }
                            }
                }

# INPUT_DATA -> connection -> output_table_name -> platform-specific configs
INPUT_DATA = {"SOURCE": {"unique_property_reference_number": {"endpoint_url": "https://api.os.uk/downloads/v1/products/OpenUPRN/downloads\?area\=GB\&format\=CSV\&redirect",
                                                              "save_to_path": "uprn.zip"
                                                              }
                        }
              }