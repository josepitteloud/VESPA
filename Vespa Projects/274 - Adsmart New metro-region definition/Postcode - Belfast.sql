--DEFINITION #1
SELECT account_number
	, CASE 	WHEN cb_address_postcode_district IN ('BT1','BT2','BT3','BT4','BT5','BT6','BT7','BT8','BT9','BT10','BT11','BT12','BT13','BT14','BT15',
											'BT16','BT17','BT18','BT19','BT20','BT21','BT22','BT23','BT24','BT25','BT26','BT27')
												THEN 'BT1'
			WHEN cb_address_postcode_district IN ('BT28','BT29','BT30','BT31','BT32','BT33','BT34','BT35','BT36','BT37','BT38','BT39','BT40','BT41',
											'BT42','BT43','BT44','BT45','BT46','BT47','BT48','BT49','BT50','BT51','BT52','BT53','BT54','BT55'
											,'BT56','BT57')
												THEN 'BT2'
			ELSE cb_address_postcode_area END postcode -- Postal area with Belfast 1 & 2 areas as Area1 BT1-BT27 AND Area2 BT28-BT57
FROM sk_prod.CUST_SINGLE_ACCOUNT_VIEW			--- REPLACE BY sk_prod.ADSMART if necessary

--DEFINITION #2
SELECT account_number
	, CASE 	WHEN cb_address_postcode_district IN ('BT1','BT2','BT3','BT4','BT5','BT10','BT11','BT12','BT13','BT14','BT15','BT16','BT17','BT18','BT19',											'BT20','BT21','BT22','BT23','BT24','BT26','BT27')
												THEN 'BT1'
			WHEN cb_address_postcode_district IN ('BT28','BT29','BT30','BT32','BT35','BT36','BT37','BT38','BT39','BT40','BT41','BT42','BT45','BT57')
												THEN 'BT2'
			ELSE cb_address_postcode_area END postcode -- Postal area with Belfast 1 & 2 areas according to new Metropolitan Area definition
FROM sk_prod.CUST_SINGLE_ACCOUNT_VIEW --- REPLACE BY sk_prod.ADSMART if necessary


/*
Postal districts not included in Belfast areas but in the Metropolitan conurbation definition: 'BT61','BT62','BT63','BT64','BT65','BT66','BT67','BT68','BT69','BT70','BT71','BT74','BT75','BT76','BT77','BT80','BT92','BT93','BT94'
*/
													
	