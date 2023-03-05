-- Create table
CREATE TABLE IF NOT EXISTS linked_ppd_epc.pricepaid_to_residential_epc
(
  transactionid text NOT NULL,
  lmk_key text NOT NULL
);

-- Delete duplicates
DELETE FROM linked_ppd_epc.pricepaid_to_residential_epc T1
USING 		linked_ppd_epc.pricepaid_to_residential_epc T2
WHERE 		T1.ctid < T2.ctid
		AND T1.transactionid = T2.transactionid
		AND T1.lmk_key = T2.lmk_key
;