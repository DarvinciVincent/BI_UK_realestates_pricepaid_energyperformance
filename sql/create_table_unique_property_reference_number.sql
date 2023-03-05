CREATE TABLE IF NOT EXISTS linked_ppd_epc.unique_property_reference_number(
uprn text,
x_coordinate string,
y_coordinate string,
latitude string,
longitude string
);

-- https://postcoder.com/docs/address-lookup/addressbase-uprn
-- https://osdatahub.os.uk/downloads/open/OpenUPRN
-- COPY linked_ppd_epc.open_uprn FROM '../data/osopenuprn_202301_csv/osopenuprn_202211.csv' DELIMITERS ',' CSV HEADER;