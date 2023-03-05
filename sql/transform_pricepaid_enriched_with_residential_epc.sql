-- Roughly 21 mins for  over 27 mils links
--/*
DROP TABLE IF EXISTS linked_ppd_epc.ppd_enriched_with_residential_epc;

CREATE TABLE linked_ppd_epc.ppd_enriched_with_residential_epc AS
	SELECT 
		ppd.transactionid,
		ppd.price,
		ppd.dateoftransfer,
		ppd.propertytype,
		ppd.oldnew,
		ppd.duration,
		ppd.paon,
		ppd.saon,
		ppd.street,
		ppd.locality,
		ppd.towncity,
		ppd.district,
		ppd.categorytype,
		ppd.recordstatus,
		epc.*,
		uprn_table.longitude,
		uprn_table.latitude
	FROM linked_ppd_epc.pricepaid_to_residential_epc AS keys
		INNER JOIN linked_ppd_epc.price_paid AS ppd
			ON keys.transactionid = ppd.transactionid
		INNER JOIN linked_ppd_epc.residential_building_epc_certificate AS epc
			ON keys.lmk_key = epc.lmk_key
		INNER JOIN linked_ppd_epc.unique_property_reference_number AS uprn_table
		ON epc.uprn = uprn_table.uprn
	ORDER BY ppd.dateoftransfer;
;

-- Delete duplicates
DELETE FROM linked_ppd_epc.pricepaid_enriched_with_residential_epc T1
USING 		linked_ppd_epc.pricepaid_enriched_with_residential_epc T2
WHERE 		T1.ctid < T2.ctid
		AND T1.transactionid = T2.transactionid
		AND T1.lmk_key = T2.lmk_key
;
