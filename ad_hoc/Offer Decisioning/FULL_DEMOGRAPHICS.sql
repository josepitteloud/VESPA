DROP TABLE MCKINSEY_FULL_DEMOGRAPHICS
GO 
/* *******************************************************************
                        FULL_DEMOGRAPHICS
********************************************************************* */

select a.cb_key_household,
       b.account_number,
       affluence,
       lifestage,
       hh_composition,
       income,
       mosaic_group,
       hh_fss_group,
       hh_number_children,
       h_age
INTO MCKINSEY_FULL_DEMOGRAPHICS	   
FROM (SELECT 
		cb_key_household, cb_row_id
		,rank() over(partition by cb_key_household   ORDER BY   cb_row_id ) as rank_hh
		,  CASE  WHEN h_affluence_v2 IN('00','01','02') THEN   '1.Very Low'
												 WHEN h_affluence_v2 IN('03','04','05') THEN   '2.Low'
												 WHEN  h_affluence_v2 IN('06','07','08') THEN   '3.Mid Low'
												 WHEN  h_affluence_v2 IN('09','10','11') THEN   '4.Mid'
												 WHEN  h_affluence_v2 IN('12','13','14') THEN   '5.Mid High'
												 WHEN  h_affluence_v2 IN('15','16','17') THEN   '6.High'
												 WHEN  h_affluence_v2 IN('18','19'     ) THEN   '7.Very High'
										   ELSE '8. Unknown' END as affluence
		,  CASE  WHEN  h_family_lifestage_2011 = '00' THEN '1.Young singles/homesharers'
												 WHEN  h_family_lifestage_2011 = '01' THEN '2.Young family no children <18'
												 WHEN  h_family_lifestage_2011 = '02' THEN '3.Young family with children <18'
												 WHEN  h_family_lifestage_2011 = '03' THEN '4.Young household with children <18'
												 WHEN  h_family_lifestage_2011 = '04' THEN '5.Mature singles/homesharers'
												 WHEN  h_family_lifestage_2011 = '05' THEN '6.Mature family no children <18'
												 WHEN  h_family_lifestage_2011 = '06' THEN '7.Mature family with children <18'
												 WHEN  h_family_lifestage_2011 = '07' THEN '8.Mature household with children <18'
												 WHEN  h_family_lifestage_2011 = '08' THEN '9.Older single'
												 WHEN  h_family_lifestage_2011 = '09' THEN '10.Older family no children <18'
												 WHEN  h_family_lifestage_2011 = '10' THEN '11.Older family/household with children<18'
												 WHEN  h_family_lifestage_2011 = '11' THEN '12.Elderly single'
												 WHEN  h_family_lifestage_2011 = '12' THEN '13.Elderly family no children <18'
												 WHEN  h_family_lifestage_2011 =  'U' THEN '14.Unclassified'
										   ELSE NULL END as lifestage
		, CASE WHEN  h_household_composition = '00' THEN '1.Families '
												WHEN  h_household_composition = '01' THEN '2.Extended family '
												WHEN  h_household_composition = '02' THEN '3.Extended household '
												WHEN  h_household_composition = '03' THEN '4.Pseudo family '
												WHEN  h_household_composition = '04' THEN '5.Single male '
												WHEN  h_household_composition = '05' THEN '6.Single female '
												WHEN  h_household_composition = '06' THEN '7.Male homesharers '
												WHEN  h_household_composition = '07' THEN '8.Female homesharers '
												WHEN  h_household_composition = '08' THEN '9.Mixed homesharers '
												WHEN  h_household_composition = '09' THEN '10.Abbreviated male families '
												WHEN  h_household_composition = '10' THEN '11.Abbreviated female families '
												WHEN  h_household_composition = '11' THEN '12.Multi-occupancy dwelling '
												WHEN  h_household_composition = 'U'  THEN '13.Unclassified '
										   ELSE NULL END as hh_composition
		,CASE  WHEN  h_income_band_v2 = '0'            THEN '<A£15,000'
												 WHEN  h_income_band_v2 = '1' THEN 'B£15,000 - £19,999'
												 WHEN  h_income_band_v2 = '2' THEN 'C£20,000 - £29,999'
												 WHEN  h_income_band_v2 = '3' THEN 'D£30,000 - £39,999'
												 WHEN  h_income_band_v2 = '4' THEN 'E£40,000 - £49,999'
												 WHEN  h_income_band_v2 = '5' THEN 'F£50,000 - £59,999'
												 WHEN  h_income_band_v2 = '6' THEN 'G£60,000 - £69,999'
												 WHEN  h_income_band_v2 = '7' THEN 'H£70,000 - £99,999'
												 WHEN  h_income_band_v2 = '8' THEN 'I£100,000 -£149,999'
												 WHEN  h_income_band_v2 = '9' THEN 'J£150,000 +'
												 WHEN  h_income_band_v2 = 'U' THEN 'Unallocated'
										   ELSE NULL END as Income

		,CASE WHEN h_mosaic_uk_group =  'A' THEN 'a.Alpha Territory'
								 WHEN h_mosaic_uk_group =  'B' THEN 'b.Professional Rewards'
								 WHEN h_mosaic_uk_group =  'C' THEN 'c.Rural Solitude'
								 WHEN h_mosaic_uk_group =  'D' THEN 'd.Small Town Diversity'
								 WHEN h_mosaic_uk_group =  'E' THEN 'e.Active Retirement'
								 WHEN h_mosaic_uk_group =  'F' THEN 'f.Suburban Mindsets'
								 WHEN h_mosaic_uk_group =  'G' THEN 'g.Careers and Kids'
								 WHEN h_mosaic_uk_group =  'H' THEN 'h.New Homemakers'
								 WHEN h_mosaic_uk_group =  'I' THEN 'i.Ex-Council Community'
								 WHEN h_mosaic_uk_group =  'J' THEN 'j.Claimant Cultures'
								 WHEN h_mosaic_uk_group =  'K' THEN 'k.Upper Floor Living'
								 WHEN h_mosaic_uk_group =  'L' THEN 'l.Elderly Needs'
								 WHEN h_mosaic_uk_group =  'M' THEN 'm.Industrial Heritage'
								 WHEN h_mosaic_uk_group =  'N' THEN 'n.Terraced Melting Pot'
								 WHEN h_mosaic_uk_group =  'O' THEN 'o.Liberal Opinions'
								 WHEN h_mosaic_uk_group =  'U' THEN 'p.Unknown'
								 ELSE null END as mosaic_group
		, CASE WHEN h_fss_group ='A'  then        'A Successful Start'
								  WHEN h_fss_group ='B'  then        'B Happy Housemates'
								  WHEN h_fss_group ='C'  then         'C Surviving Singles'
								  WHEN h_fss_group ='D'  then        'D On the Bread Line'
								  WHEN h_fss_group ='E'  then         'E Flourishing Families'
								  WHEN h_fss_group ='F'  then         'F Credit-hungry Families'
								  WHEN h_fss_group ='G'  then         'G Gilt-edged Lifestyles'
								  WHEN h_fss_group ='H'  then          'H Mid-life Affluence'
								  WHEN h_fss_group ='I'  then          'I Modest Mid-years'
								  WHEN h_fss_group ='J'  then          'J Advancing Status'
								  WHEN h_fss_group ='K'  then          'K Ageing Workers'
								  WHEN h_fss_group ='L'  then          'L Wealthy Retirement'
								  WHEN h_fss_group ='M'  then          'M Elderly Deprivation'
								  WHEN h_fss_group ='U'  then           'U Unclassified'
								 ELSE null END as hh_FSS_group
		,CASE   WHEN  h_number_of_children_in_household_2011 ='0' then      'No children'
				WHEN  h_number_of_children_in_household_2011 ='1' then      '1 child'
				WHEN  h_number_of_children_in_household_2011 ='2' then      '2 children'
				WHEN  h_number_of_children_in_household_2011 ='3' then      '3 children'
				WHEN  h_number_of_children_in_household_2011 ='4' then      '4 or more children'
				WHEN  h_number_of_children_in_household_2011 ='U' then      'Unclassified'
						ELSE null END as hh_number_children
		,CASE WHEN h_age_coarse='0'     then  'Age 18-25'
				WHEN h_age_coarse='1'   then  'Age 26-35'
				WHEN h_age_coarse='2'   then  'Age 36-45'
				WHEN h_age_coarse='3'   then  'Age 46-55'
				WHEN h_age_coarse='4'   then  'Age 56-65'
				WHEN h_age_coarse='5'   then  'Age 66+'
				WHEN h_age_coarse='U'   then  'Unclassified'
		 ELSE null END as h_age
		 , cb_address_postcode postcode
		 
	INTO Demographics_McKinsey
	FROM  experian_consumerview ) AS a
JOIN cust_single_account_view b on a.cb_key_household = b.cb_key_household
WHERE rank_hh = 1 
GROUP BY 
       a.cb_key_household,
       b.account_number,
       affluence,
       lifestage,
       hh_composition,
       income,
       mosaic_group,
       hh_fss_group,
       hh_number_children,
       h_age,
	   postcode

COMMIT 
GRANT SELECT ON MCKINSEY_FULL_DEMOGRAPHICS TO vespa_group_low_security, rko04, citeam

GO


	   
	   
	   
	   