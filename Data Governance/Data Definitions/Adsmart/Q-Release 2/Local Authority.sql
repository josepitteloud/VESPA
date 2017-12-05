------------------- Local authority
---- REPLACE ##Local_authority## by the real field name
---- REPLACE ##Local_authorityTABLE## by the real field name
---- REPLACE ##ADSMART## by the final Adsmart table

UPDATE ##ADSMART##
SET  LOCAL_AUTHORITY = Ladnm
FROM ##ADSMART## AS a 
JOIN ##Local_authorityTABLE## AS b ON REPLACE(TRIM(a.cb_address_postcode),' ','' = REPLACE(TRIM(b.cb_address_postcode),' ', '')
COMMIT

