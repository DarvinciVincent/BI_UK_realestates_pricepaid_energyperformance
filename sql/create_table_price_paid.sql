-- This file is for importing price paid data


-- Create table
CREATE TABLE IF NOT EXISTS linked_ppd_epc.price_paid
(
  transactionid text NOT NULL,
  price bigint,
  dateoftransfer date,
  postcode text,
  propertytype text,
  oldnew text,
  duration text,
  paon text,
  saon text,
  street text,
  locality text,
  towncity text,
  district text,
  county text,
  categorytype text,
  recordstatus text
);

-- 2/ Import data from csv to database
-- COPY pricepaid FROM '../data/pp-complete.csv' DELIMITERS ',' CSV QUOTE '"';

-- 3/ Clean the Land Registry PPD before the do the linkage with EPC
-- 3.1/ Clean transactions which are not sold at full market value
-- DELETE FROM  pricepaid WHERE categorytype='B';
-- 3.2/ Clean transactions for which the property type is 'Other'
-- DELETE FROM  pricepaid WHERE propertytype='O';