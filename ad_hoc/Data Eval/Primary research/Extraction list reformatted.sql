TRUNCATE TABLE PR_SRV_log1_O
INSERT INTO PR_SRV_log0
SELECt top 10 * from PR_SRV_log1_O WITH (NOLOCK)
commit;

SELECT  top 10 subscriber_id, count(*)
FROM PR_SRV_log2_S
GROUP BY subscriber_id
HAVING COUNT(*) between 27 AND 30

account_number	subscriber_id	mth	logs	Q_flag	date_stamp

INSERT INTO PR_SRV_Panel_acct_logs_by_month

SELECT  
     sbo.account_number
    , a.subscriber_id
    , 9 as mth
    , count(*)
    , 0 as Q_flag
    , getdate()
FROM PR_SRV_log2_S as a
JOIN  vespa_analysts.vespa_single_box_view as sbo ON  a.subscriber_id = sbo.subscriber_id
GROUP BY a.subscriber_id  
        , sbo.account_number
        
        
        
        
        
        
SELECT 
	  cust_email_address
	, account_number
	, h_affluence
	, cb_key_household
	, h_lifestage
	, household_composition
	, mosaic_segments
	, Count(*) 
INTO PR_SRV_EMAIL_1	
FROM   	sk_prod.cust_single_account_view 
WHERE  ( ( ( ( [cust_single_account_view].[cust_email_address] LIKE '%@%' ) 						--Valid mail add with @
    AND ( [cust_single_account_view].[cust_email_address] IS NOT NULL ) ) 							--Not null emails
	AND NOT ( ( ( [cust_single_account_view].[cust_email_address] = 'beeesneees@gmail.com' ) 		--LIST of banned mails	
				AND [cust_single_account_view].[cust_email_address] IS NOT NULL ) 
                OR ( ( [cust_single_account_view].[cust_email_address] = 'dan.winters@monkrat.com' ) 
                    AND [cust_single_account_view].[cust_email_address] IS NOT NULL) 
                OR ( ( [cust_single_account_view].[cust_email_address] = 'tony.email@ntlworld.com' ) 
                    AND [cust_single_account_view].[cust_email_address] IS NOT NULL ) 
                OR ( ( [cust_single_account_view].[cust_email_address] = 'allan.lloyd@blueyonder.co.uk' ) 
                    AND [cust_single_account_view].[cust_email_address] IS NOT NULL ) 
                OR ( ( [cust_single_account_view].[cust_email_address] = 'melanie@melfcomputing.com' ) 
                    AND [cust_single_account_view].[cust_email_address] IS NOT NULL ) 
                OR ( ( [cust_single_account_view].[cust_email_address] = 'ronaldthomas02@aol.com' ) 
                    AND [cust_single_account_view].[cust_email_address] IS NOT NULL ) 
                OR ( ( [cust_single_account_view].[cust_email_address] = 'butemilylovedhim@tiscali.co.uk' ) 
                    AND [cust_single_account_view].[cust_email_address] IS NOT NULL ) 
                OR ( ( [cust_single_account_view].[cust_email_address] = 'mark@grondar.org' ) 
                    AND [cust_single_account_view].[cust_email_address] IS NOT NULL ) 
                OR ( ( [cust_single_account_view].[cust_email_address] = 'nick.clews@yahoo.co.uk' ) 
                    AND [cust_single_account_view].[cust_email_address] IS NOT NULL ) 
                OR ( ( [cust_single_account_view].[cust_email_address] = 'sarahjbrake@yahoo.co.uk' ) 
                    AND [cust_single_account_view].[cust_email_address] IS NOT NULL ) 
                OR ( ( [cust_single_account_view].[cust_email_address] = 'mel-tena@tiscali.co.uk     ' ) 
                    AND [cust_single_account_view].[cust_email_address] IS NOT NULL ) 
                OR ( ( [cust_single_account_view].[cust_email_address] = 'kevin.van-biene@cps.gsi.gov.uk' ) 
                    AND [cust_single_account_view].[cust_email_address] IS NOT NULL ) 
                OR ( ( [cust_single_account_view].[cust_email_address] = 'peter@clevermed.com' ) 
                    AND [cust_single_account_view].[cust_email_address] IS NOT NULL ) 
                OR ( ( [cust_single_account_view].[cust_email_address] = 'joerea@btinternet.com' ) 
                    AND [cust_single_account_view].[cust_email_address] IS NOT NULL ) ) 
				OR ( ( [cust_single_account_view].[cust_email_address] = 'dgbarclay@hotmail.com' ) 
                     AND [cust_single_account_view].[cust_email_address] IS NOT NULL ) ) 
         AND ( [cust_single_account_view].[cust_email_address] NOT IN 	
                     (SELECT x_email_address 													-- Banned mails from ak_prod email_address table where status in fail or DMMY
					  FROM sk_prod.email_address 
                      WHERE [email_address].[x_email_status_code] IN ( 'FAIL', 'DMMY' ) 
                              UNION ALL 
                      SELECT user_field_1 														--Dormant emails from sk_prod.cust_list_matching 
                      FROM   sk_prod.cust_list_matching 
                      WHERE  ( [cust_list_matching].[source_code] = 'DORMANT_SKY_EMAILS' ) 
                        AND ( Datediff(day, Cast( [cust_list_matching].[user_field_2] AS DATE), Today(*)) >= 180 )) 
						AND [cust_single_account_view].[emailaddress] NOT IN ( 
												SELECT [email_master_suppressions].[emailaddress] 		-- EXCLUDING sk_prod.email_master_suppressions 
                                                FROM sk_prod.email_master_suppressions 
                          /*                         UNION ALL 
												SELECT [email_unsubs_suppressions].[emailaddress] 		-- EXCLUDING sk_prod.email_unsubs_suppressions 
                                                FROM sk_prod.email_unsubs_suppressions 
                                                WHERE [email_unsubs_suppressions].[reason] IN ( 'a', 'c', 'g', 'i', 's', 'w', 'e', 'r', 'k', 'd', 'p', 'b' ) */
													UNION ALL 
												SELECT email_address 
												FROM   sk_prod.cust_survey_suppressions 				-- EXCLUDING sk_prod.cust_survey_suppressions
												WHERE  ( [cust_survey_suppressions].[event] LIKE 'EMAIL%') 
													AND ( [cust_survey_suppressions].[mobile_number] IS NOT NULL )) 
						AND [cust_single_account_view].[account_number] NOT IN (
												SELECT account_number 									--
												FROM sk_prod.cust_survey_suppressions 
												WHERE  ( [cust_survey_suppressions].[event] LIKE 'EMAIL%' ) 
													AND ([cust_survey_suppressions].[mobile_number] IS NOT NULL )) 
													AND [cust_single_account_view].[account_number] NOT IN (
																	SELECT account_number 						-- EXCLUDING duplicate emails
																	FROM sk_prod.cust_single_account_view 
																	WHERE  ( [cust_single_account_view].[emailaddress] IN (
																					SELECT [cust_single_account_view].[emailaddress] 
																					FROM sk_prod.cust_single_account_view 
																					GROUP  BY 
																					[cust_single_account_view].[emailaddress] 
																					HAVING Count(*) > 1) ) 
																		OR ( [cust_single_account_view].[account_number] IN (
																					SELECT [cust_single_account_view].[account_number] -- EXCLUDING duplicate accounts
																					FROM sk_prod.cust_single_account_view 
																					GROUP  BY 
																						[cust_single_account_view].[account_number] 
																					HAVING Count(*) > 1) )) ) ); 


----------
TRUNCATE TABLE PR_SRV_accounts_with_logs
commit;
INSERT INTO PR_SRV_accounts_with_logs
SELECT 
	  b.account_number
	, a.subscriber_id 
    , COUNT(1) as Logs
FROM vespa_analysts.panel_data as a
JOIN vespa_analysts.vespa_single_box_view as sbo ON  a.subscriber_id = sbo.subscriber_id
WHERE a.data_received = 1
	AND dt BETWEEN '2013-09-01' and '2013-10-31 23:59:59'
GROUP BY 
	  b.account_number
	, a.subscriber_id 


SELECT subscriber_id , count(1) hits
FROM vespa_analysts.vespa_single_box_view as sbo
GROUP BY subscriber_id
HAVING hits > 1


---- FINAL EMAIL LIST
SELECT 
    a.subscriber_id
  , a.logs
  , b.* 
  , sav.prod_active_broadband_package_desc
  , sav.current_package
INTO PR_SRV_Email_list_1
FROM PR_SRV_accounts_with_logs AS a
JOIN PR_SRV_EMAIL_1 as b    ON a.account_number = b.account_number
JOIN  sk_prod.cust_single_account_view AS sav ON b.account_number = sav.account_number
Where a.logs >= 30
commit;



SELECT count(*)

FROM PR_SRV_accounts_with_logs AS a
JOIN PR_SRV_EMAIL_1 as b    ON a.account_number = b.account_number
JOIN  sk_prod.cust_single_account_view AS sav ON b.account_number = sav.account_number
Where a.logs >= 30



SELECT survey 
, count(*) hit 
from pitteloudj.PR_SRV_Email_list_2 
GROUP BY survey 


SELECT top 10 * 
FROM pitteloudj.PR_SRV_Email_list_2 as l2
join pitteloudj.PR_SRV_content_2 as c2 ON c2.subs_Id = l2.subscriber_id AND c2.email = l2.cust_email_address
AND cast(c2.acct as varchar) = l2.account_number
WHERE survey is not null


UPDATE pitteloudj.PR_SRV_Email_list_2 
SET survey = 'Content Tracker Extra 10k'
FROM pitteloudj.PR_SRV_Email_list_2 as l2
join pitteloudj.PR_SRV_content_2 as c2 ON c2.subs_Id = l2.subscriber_id AND c2.email = l2.cust_email_address
AND cast(c2.acct as varchar) = l2.account_number

COmmit


SELECT *
into PR_SRV_Content_list
FROM PR_SRV_Email_list_2
WHERE lower(survey) like 'content%' 

GRANT ALL ON PR_SRV_Content_list TO prescol WITH GRANT OPTION