-------------------------------------------------------------------------------------------------------
-- STB_PRE_REGISTRATION                 
-------------------------------------------------------------------------------------------------------

-- 2015-04-28 - DO NOT RUN THIS CODE - DATA NOT AVAILABLE IN OLIVE YET!!!!! 
-- CHECK 'YES' DEFINITION WITH PMs
/* ********************************************************
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
*/
        SELECT account_number
                , CASE WHEN registration_status = 'Registered' THEN 'YES'
                        WHEN registration_status = 'Validated' THEN 'YES'
                        WHEN registration_status = 'Deregistered' THEN 'NO' END as STB_PRE_REGISTRATION
        INTO quarterly_release_1_STB_PRE_REGISTRATION
        FROM ETHAN_REGISTRATIONS_UK_CUSTOMERS
        GROUP BY account_number
                , STB_PRE_REGISTRATION
        ;
        commit;

        INSERT INTO quarterly_release_1_STB_PRE_REGISTRATION
        SELECT account_number
                , CASE WHEN registration_status = 'Registered' THEN 'YES'
                        WHEN registration_status = 'Validated' THEN 'YES'
                        WHEN registration_status = 'Deregistered' THEN 'NO' END as STB_PRE_REGISTRATION
        FROM ETHAN_REGISTRATIONS_ROI_CUSTOMERS
        GROUP BY account_number
                , registration_status
        ;
        commit;

        UPDATE pm_quarterly_release_1_adsmart
        SET a.STB_PRE_REGISTRATION = COALESCE(STB_PRE_REGISTRATION, 'Unknown')
        FROM pm_quarterly_release_1_adsmart a
        LEFT JOIN quarterly_release_1_STB_PRE_REGISTRATION b ON a.account_number = b.account_number
        ;
        commit;
        DROP TABLE quarterly_release_1_STB_PRE_REGISTRATION;
        commit;