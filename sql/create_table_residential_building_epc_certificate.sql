-- This file is for importing EPC data
-- DROP TABLE epc_raw;
-- 1 Create table
CREATE TABLE IF NOT EXISTS linked_ppd_epc.residential_building_epc_certificate
(
  LMK_KEY text NOT NULL,
  ADDRESS1 text,
  ADDRESS2 text,
  ADDRESS3 text,
  POSTCODE varchar(9),
  BUILDING_REFERENCE_NUMBER text,
  CURRENT_ENERGY_RATING char(1),
  POTENTIAL_ENERGY_RATING char(1),
  CURRENT_ENERGY_EFFICIENCY float,
  POTENTIAL_ENERGY_EFFICIENCY float,
  PROPERTY_TYPE text,
  BUILT_FORM text,
  INSPECTION_DATE date,
  LOCAL_AUTHORITY varchar(12), --same for all
  CONSTITUENCY varchar(12), --same for all
  COUNTY text, --same for all
  LODGEMENT_DATE date,
  TRANSACTION_TYPE text,
  ENVIRONMENT_IMPACT_CURRENT float,
  ENVIRONMENT_IMPACT_POTENTIAL float,
  ENERGY_CONSUMPTION_CURRENT float,
  ENERGY_CONSUMPTION_POTENTIAL float,
  CO2_EMISSIONS_CURRENT float,
  CO2_EMISS_CURR_PER_FLOOR_AREA float,
  CO2_EMISSIONS_POTENTIAL float,
  LIGHTING_COST_CURRENT float,
  LIGHTING_COST_POTENTIAL float,
  HEATING_COST_CURRENT float,
  HEATING_COST_POTENTIAL float,
  HOT_WATER_COST_CURRENT float,
  HOT_WATER_COST_POTENTIAL float,
  TOTAL_FLOOR_AREA float,
  ENERGY_TARIFF text,
  MAINS_GAS_FLAG char(1), -- Y or N
  FLOOR_LEVEL varchar(15), -- include NODATA!, 01, 1st, Ground, basement... NEED STANDARDIZATION
  FLAT_TOP_STOREY char(1), -- Y or N
  FLAT_STOREY_COUNT float, -- include 2.0...
  MAIN_HEATING_CONTROLS float, -- 2104...
  MULTI_GLAZE_PROPORTION float, -- 100, 85
  GLAZED_TYPE text, -- double glazing installed before 2002, unknown install date..
  GLAZED_AREA text,
  EXTENSION_COUNT float,
  NUMBER_HABITABLE_ROOMS float,
  NUMBER_HEATED_ROOMS float,
  LOW_ENERGY_LIGHTING float,
  NUMBER_OPEN_FIREPLACES float,
  HOTWATER_DESCRIPTION text,
  HOT_WATER_ENERGY_EFF varchar(10), -- Good, Very Poor...
  HOT_WATER_ENV_EFF varchar(10), --same
  FLOOR_DESCRIPTION text,
  FLOOR_ENERGY_EFF varchar(10), --same, N/A, NO DATA!
  FLOOR_ENV_EFF varchar(10), --same
  WINDOWS_DESCRIPTION text,
  WINDOWS_ENERGY_EFF varchar(10), -- Good, Very Poor...
  WINDOWS_ENV_EFF varchar(10), -- Good, Very Poor...
  WALLS_DESCRIPTION text,
  WALLS_ENERGY_EFF varchar(10), -- Good, Very Poor...
  WALLS_ENV_EFF varchar(10), -- Good, Very Poor...
  SECONDHEAT_DESCRIPTION text, -- include None
  SHEATING_ENERGY_EFF varchar(10), -- Good, Very Poor...N/A all?
  SHEATING_ENV_EFF varchar(10), -- Good, Very Poor... N/A all?
  ROOF_DESCRIPTION text,
  ROOF_ENERGY_EFF varchar(10), -- Good, Very Poor...
  ROOF_ENV_EFF varchar(10), -- Good, Very Poor...
  MAINHEAT_DESCRIPTION text,
  MAINHEAT_ENERGY_EFF varchar(10), -- Good, Very Poor...
  MAINHEAT_ENV_EFF varchar(10), -- Good, Very Poor...
  MAINHEATCONT_DESCRIPTION text,
  MAINHEATC_ENERGY_EFF varchar(10), -- Good, Very Poor...
  MAINHEATC_ENV_EFF varchar(10), -- Good, Very Poor...
  LIGHTING_DESCRIPTION text,
  LIGHTING_ENERGY_EFF varchar(10), -- Good, Very Poor...
  LIGHTING_ENV_EFF varchar(10), -- Good, Very Poor...
  MAIN_FUEL text,
  WIND_TURBINE_COUNT float,
  HEAT_LOSS_CORRIDOR text, -- heated corridor, NO DATA!
  UNHEATED_CORRIDOR_LENGTH float, -- may include in text type
  FLOOR_HEIGHT float,
  PHOTO_SUPPLY float,
  SOLAR_WATER_HEATING_FLAG char(1),
  MECHANICAL_VENTILATION text,
  ADDRESS text, -- can be in capital, can include county, can include street name twice, can include flat no, flat, apartment, ...
  LOCAL_AUTHORITY_LABEL text, -- town name
  CONSTITUENCY_LABEL text, -- town name
  POSTTOWN text, -- NEED STANDARDIZATION, CAPITALIZE?
  CONSTRUCTION_AGE_BAND text, --England and Wales: 1983-1990
  LODGEMENT_DATETIME timestamp,
  TENURE text, -- rented, owner...
  FIXED_LIGHTING_OUTLETS_COUNT float,
  LOW_ENERGY_FIXED_LIGHT_COUNT float,
  UPRN text, --100110786110
  UPRN_SOURCE text
);