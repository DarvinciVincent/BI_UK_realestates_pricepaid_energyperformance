import os
from dotenv import load_dotenv

# ENVIRONMENT VARIABLES
load_dotenv()

### DATABASE
DB_URL = str(os.getenv("DB_URL"))


# RUNTIME CONFIGURATIONS
COLUMN_TYPES = {"unique_property_reference_number": {"uprn": "string",
                                                     "x_coordinate": "string",
                                                     "y_coordinate": "string",
                                                     "latitude": "string",
                                                     "longitude": "string"
                                                     }
                }

NAN_PATTERNS = ["N/A", "NaN", "nan", "unknown", "UNKNOWN", "Unknown", "INVALID!", "NODATA!", "NO DATA!", ""]

# OUTPUT_DATA -> connection -> output_table_name -> bucket, country, subject, source, table_name, path_date_params, file_extension, column_types
OUTPUT_DATA =  {"DB": {"unique_property_reference_number": {"table_name": "unique_property_reference_number",
                                                            "db_schema": "linked_ppd_epc",
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