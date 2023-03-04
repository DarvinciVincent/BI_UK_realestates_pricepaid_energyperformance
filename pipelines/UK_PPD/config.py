import os
from dotenv import load_dotenv

from pipelines.UK_EPC.config import OUTPUT_DATA as EPC_OUTPUT_DATA

# ENVIRONMENT VARIABLES
load_dotenv()

### DATABASE
DB_URL = str(os.getenv("DB_URL"))


# RUNTIME CONFIGURATIONS

# PPD data is available from this year
PPD_MIN_YEAR = 1995

COLUMN_TYPES = {"price_paid":  {"transactionid": "string",
                                "price": int,
                                "dateoftransfer": "datetime64[ns]",
                                "postcode": "string",
                                "propertytype": "string",
                                "oldnew": "string",
                                "duration": "string",
                                "paon": "string",
                                "saon": "string",
                                "street": "string",
                                "locality": "string",
                                "towncity": "string",
                                "district": "string",
                                "county": "string",
                                "categorytype": "string",
                                "recordstatus": "string"
                                },

                "pricepaid_to_residential_epc": {"transactionid": "string",
                                                 "lmk_key": "string"},

                "pricepaid_enriched_with_residential_epc": {"transactionid": "string",
                                                            "price": int,
                                                            "dateoftransfer": "datetime64[ns]",
                                                            "propertytype": "string",
                                                            "oldnew": "string",
                                                            "duration": "string",
                                                            "paon": "string",
                                                            "saon": "string",
                                                            "street": "string",
                                                            "locality": "string",
                                                            "towncity": "string",
                                                            "district": "string",
                                                            "categorytype": "string",
                                                            "recordstatus": "string",
                                                            "lmk_key": "string",
                                                            "address1": "string",
                                                            "address2": "string",
                                                            "address3": "string",
                                                            "postcode": "string",
                                                            "building_reference_number": int,
                                                            "current_energy_rating": "string",
                                                            "potential_energy_rating": "string",
                                                            "current_energy_efficiency": float,
                                                            "potential_energy_efficiency": float,
                                                            "property_type": "string",
                                                            "built_form": "string",
                                                            "inspection_date": "datetime64[ns]",
                                                            "local_authority": "string",
                                                            "constituency": "string",
                                                            "county": "string",
                                                            "lodgement_date": "string",
                                                            "transaction_type": "string",
                                                            "environment_impact_current": float,
                                                            "environment_impact_potential": float,
                                                            "energy_consumption_current": float,
                                                            "energy_consumption_potential": float,
                                                            "co2_emissions_current": float,
                                                            "co2_emiss_curr_per_floor_area": float,
                                                            "co2_emissions_potential": float,
                                                            "lighting_cost_current": float,
                                                            "lighting_cost_potential": float,
                                                            "heating_cost_current": float,
                                                            "heating_cost_potential": float,
                                                            "hot_water_cost_current": float,
                                                            "hot_water_cost_potential": float,
                                                            "total_floor_area": float,
                                                            "energy_tariff": "string",
                                                            "mains_gas_flag": "string",
                                                            "floor_level": "string",
                                                            "flat_top_storey": "string",
                                                            "flat_storey_count": float,
                                                            "main_heating_controls": float,
                                                            "multi_glaze_proportion": float,
                                                            "glazed_type": "string",
                                                            "glazed_area": "string",
                                                            "extension_count": float,
                                                            "number_habitable_rooms": float,
                                                            "number_heated_rooms": float,
                                                            "low_energy_lighting": float,
                                                            "number_open_fireplaces": float,
                                                            "hotwater_description": "string",
                                                            "hot_water_energy_eff": "string",
                                                            "hot_water_env_eff": "string",
                                                            "floor_description": "string",
                                                            "floor_energy_eff": "string",
                                                            "floor_env_eff": "string",
                                                            "windows_description": "string",
                                                            "windows_energy_eff": "string",
                                                            "windows_env_eff": "string",
                                                            "walls_description": "string",
                                                            "walls_energy_eff": "string",
                                                            "walls_env_eff": "string",
                                                            "secondheat_description": "string",
                                                            "sheating_energy_eff": "string",
                                                            "sheating_env_eff": "string",
                                                            "roof_description": "string",
                                                            "roof_energy_eff": "string",
                                                            "roof_env_eff": "string",
                                                            "mainheat_description": "string",
                                                            "mainheat_energy_eff": "string",
                                                            "mainheat_env_eff": "string",
                                                            "mainheatcont_description": "string",
                                                            "mainheatc_energy_eff": "string",
                                                            "mainheatc_env_eff": "string",
                                                            "lighting_description": "string",
                                                            "lighting_energy_eff": "string",
                                                            "lighting_env_eff": "string",
                                                            "main_fuel": "string",
                                                            "wind_turbine_count": float,
                                                            "heat_loss_corridor": "string",
                                                            "unheated_corridor_length": float,
                                                            "floor_height": float,
                                                            "photo_supply": float,
                                                            "solar_water_heating_flag": "string",
                                                            "mechanical_ventilation": "string",
                                                            "address": "string",
                                                            "local_authority_label": "string",
                                                            "constituency_label": "string",
                                                            "posttown": "string",
                                                            "construction_age_band": "string",
                                                            "lodgement_datetime": "datetime64[ns]",
                                                            "tenure": "string",
                                                            "fixed_lighting_outlets_count": float,
                                                            "low_energy_fixed_light_count": float,
                                                            "uprn": "string",
                                                            "uprn_source": "string",
                                                            #"latitude": "string",   # remove?
                                                            #"longitude": "string"   # remove?
                                                            }
                }

NAN_PATTERNS = ["N/A", "NaN", "nan", "unknown", "UNKNOWN", "Unknown", "INVALID!", "NODATA!", "NO DATA!", ""]

# OUTPUT_DATA -> connection -> output_table_name -> bucket, country, subject, source, table_name, path_date_params, file_extension, column_types
OUTPUT_DATA = {"DB":   {"price_paid":  {"table_name": "price_paid",
                                        "db_schema": "linked_ppd_epc",
                                        "column_types": COLUMN_TYPES["price_paid"]
                                        },
                      
                        "pricepaid_to_residential_epc": {"table_name": "pricepaid_to_residential_epc",
                                                         "db_schema": "linked_ppd_epc",
                                                         "column_types": COLUMN_TYPES["pricepaid_to_residential_epc"]
                                                        },
                        
                        "pricepaid_enriched_with_residential_epc": {"table_name": "pricepaid_enriched_with_residential_epc",
                                                                    "db_schema": "linked_ppd_epc",
                                                                    "column_types": COLUMN_TYPES["pricepaid_enriched_with_residential_epc"]
                                                                    },
                        }
                }

# INPUT_DATA -> connection -> output_table_name -> platform-specific configs
INPUT_DATA = {"SOURCE": {"price_paid": {"endpoint_url_types": ["complete", "yearly", "half_yearly", "current_month"],
                                        "endpoint_url_current_month": "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv",
                                        "endpoint_url_complete": "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv",
                                        "endpoint_url_yearly": "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-<YEAR>.csv",
                                        "endpoint_url_half_yearly": "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-<YEAR>-part<PART_NUMBER>.csv",
                                        }
                        },

              "DB": {"price_paid": OUTPUT_DATA["DB"]["price_paid"],
                     
                        "residential_energy_performance_certificate": EPC_OUTPUT_DATA["DB"]["residential_energy_performance_certificate"],

                        "pricepaid_to_residential_epc": OUTPUT_DATA["DB"]["pricepaid_to_residential_epc"]
                        }
              }

# Update params for INPUT_DATA
INPUT_DATA["DB"]["price_paid"]["needed_columns"] = ["transactionid", "postcode", "propertytype", "paon", "saon", "street", "locality"]
INPUT_DATA["DB"]["residential_energy_performance_certificate"]["needed_columns"] = ["lmk_key", "address1", "address2", "address3", "postcode", "property_type", "address"]
