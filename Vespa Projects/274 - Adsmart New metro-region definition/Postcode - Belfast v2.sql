/* **************************************
* 		ADSMART Postal Area Definition
* 		Notes: 
*			- Replace #####ADSMART_TABLE#### 	by the Adsmart table
*			- Replace ###POSTAL_AREA_FIELD #### by the Postal Area 
*			- Replace ### UNKNOWN_VALUE ###
*			- Belfast BT1: Composed by Postal districts BT1 to BT27
*			- Belfast BT2: Composed by Postal districts BT28 to BT57
*			- Any other valid postal area get the postal area
*			- Non-Valid Postal Areas get the "Unknown" value
*
*
*		Coded by : Jose Pitteloud
*		Date: 04-12-2014
*********************************************** */


UPDATE 		#####ADSMART_TABLE####
SET 		###POSTAL_AREA_FIELD ####  = CASE 	WHEN cb_address_postcode_district IN ('BT1','BT2','BT3','BT4','BT5','BT6','BT7','BT8','BT9','BT10','BT11','BT12','BT13','BT14','BT15',
																	'BT16','BT17','BT18','BT19','BT20','BT21','BT22','BT23','BT24','BT25','BT26','BT27')
																		THEN 'BT1'
												WHEN cb_address_postcode_district IN ('BT28','BT29','BT30','BT31','BT32','BT33','BT34','BT35','BT36','BT37','BT38','BT39','BT40','BT41',
																	'BT42','BT43','BT44','BT45','BT46','BT47','BT48','BT49','BT50','BT51','BT52','BT53','BT54','BT55'
																	,'BT56','BT57')
																		THEN 'BT2'
												WHEN cb_address_postcode_area IN ('AB','AL','B','BA','BB','BD','BH','BL','BN','BR','BS','BT','CA','CB','CF','CH',
																					'CM','CO','CR','CT','CV','CW','DA','DD','DE','DG','DH','DL','DN','DT','DY','E',
																					'EC','EH','EN','EX','FK','FY','G','GL','GU','HA','HD','HG','HP','HR','HS','HU',
																					'HX','IG','IP','IV','KA','KT','KW','KY','L','LA','LD','LE','LL','LN','LS','LU',
																					'M','ME','MK','ML','N','NE','NG','NN','NP','NR','NW','OL','OX','PA','PE','PH',
																					'PL','PO','PR','RG','RH','RM','S','SA','SE','SG','SK','SL','SM','SN','SO','SP',
																					'SR','SS','ST','SW','SY','TA','TD','TF','TN','TQ','TR','TS','TW','UB','W','WA',
																					'WC','WD','WF','WN','WR','WS','WV','YO','ZE','GY','JE','IM')
																		THEN cb_address_postcode_area
												ELSE '### UNKNOWN_VALUE ###' END 



