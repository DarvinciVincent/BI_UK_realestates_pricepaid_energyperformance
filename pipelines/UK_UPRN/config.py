import os
from dotenv import load_dotenv

# ENVIRONMENT VARIABLES
load_dotenv()

### MYSQL
MYSQL_HOST = os.getenv("POSTGRES_HOST")


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
OUTPUT_DATA = {"S3": {"unique_property_reference_number":  {"bucket": "data-engineering-raw-data",
                                                            "country": "united_kingdom",
                                                            "subject": "real_estate_geo_details",
                                                            "source": "national_archives_of_all_of_Great_Britain",
                                                            "table_name": "unique_property_reference_number",
                                                            "path_date_params": ["year", "month", "extract_date"],   # UPDATE YEAR
                                                            "file_extension": ".parquet",
                                                            "column_types": COLUMN_TYPES["unique_property_reference_number"]
                                                            #s3://data-engineering-raw-data/united_kingdom/real_estate_geo_details/national_archives_of_all_of_Great_Britain/year=<YEAR>/month=<MONTH>/unique_property_reference_number_<EXTRACTDATE>.parquet
                                                            }
                       }
                }


# INPUT_DATA -> connection -> output_table_name -> platform-specific configs
INPUT_DATA = {"SOURCE": {"unique_property_reference_number": {"endpoint_url": "https://api.os.uk/downloads/v1/products/OpenUPRN/downloads\?area\=GB\&format\=CSV\&redirect",
                                                              "save_to_path": "uprn.zip"
                                                              }
                        }
              }