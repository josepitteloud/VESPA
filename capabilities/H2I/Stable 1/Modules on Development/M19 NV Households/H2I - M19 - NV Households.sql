-- alter table V289_M08_SKY_HH_COMPOSITION add  nonviewer_household   TINYINT
-- select

-- exec v289_M19_Non_Viewing_Households '2014-09-23'

 /*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:                                                 Skyview H2I
**Analysts:                             Angel Donnarumma        (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson          (Jason.Thompson@skyiq.co.uk)
                                                                                ,Hoi Yu Tang            (HoiYu.Tang@skyiq.co.uk)
                                                                                ,Jose Pitteloud         (jose.pitteloud@skyiq.co.uk)
**Stakeholder:                          SkyIQ
                                                                                ,Jose Loureda           (Jose.Loureda@skyiq.co.uk)
**Due Date:                             11/07/2014
**Project Code (Insight Collation):     v289
**Sharepoint Folder:    

        http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FSIGEvolved%2FShared%20Documents%2F01%20Analysis%20Requests%2FV289%20-%20Skyview%20Futures%2F01%20Plans%20Briefs%20and%20Project%20Admin                                                        
                                                                  
**Business Brief:

        This Module goal is to extract and prepare the household and individual data from Experian 

**Module:
        
        M08: Experian Data Preparation
                        M08.0 - Initialising Environment
                        M08.1 - Account extraction from SAV
                        M08.2 - Experian HH Info Extraction (1st round - Only hh_key and address line matching accounts)
                        M08.3 - Experian HH Info Extraction (2nd round - Non-matching address line accounts AND hh > 10 people) 
                        M08.4 - Experian HH Info Extraction (3nd round - Non-matching address line accounts)
                        M08.5 - Individual TABLE POPULATION
                        M08.6 - Add Head of Household
                        M08.7 - Add Individual Children
                        M08.8 - Final Tidying of Data
                
--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M19.0 - Initialising Environment
-----------------------------------

create or replace procedure v289_M19_Non_Viewing_Households
        @processing_day date

AS BEGIN

        MESSAGE cast(now() as timestamp)||' | Begining M19.0 - Initialising Environment' TO CLIENT



        MESSAGE cast(now() as timestamp)||' | @ M19.0: Initialising Environment DONE' TO CLIENT


--------------------------------------
-- M19.1 - Non Viewing Household Profile From Barb
--------------------------------------

        MESSAGE cast(now() as timestamp)||' | Begining M19.1 - NV viewing hhd profile from Barb' TO CLIENT



        -- Get hhds from Barb who have viewed tv for the processing day
        select  distinct household_number
        into    #viewing_hhds
        from    skybarb_fullview
        where   date(start_time_of_session)     =       @processing_day
        COMMIT -- (^_^)

        create hg index ind1 on #viewing_hhds(household_number) COMMIT -- (^_^)



        -- Get details on individuals from Sky households in BARB (no date-dependency here)
        select
                        h.house_id                              as      household_number
                ,       h.person                                as      person_number
                ,       h.age
                ,       case
                                when age <= 19                  then 'U'
                                when h.sex = 'Male'     then 'M'
                                when h.sex = 'Female'   then 'F'
                        end                                             as      gender
                ,       case
                                when age <= 11                          then '0-11'
                                WHEN age BETWEEN 12 AND 19      then '12-19'
                                WHEN age BETWEEN 20 AND 24      then '20-24'
                                WHEN age BETWEEN 25 AND 34      then '25-34'
                                WHEN age BETWEEN 35 AND 44      then '35-44'
                                WHEN age BETWEEN 45 AND 64      then '45-64'
                                WHEN age >= 65                          then '65+'
                        end                                             as      ageband
              --  ,       cast(h.head as char(1)) as      head_of_hhd
                ,       count(1) over (partition by h.house_id) as hhsize
                ,       w.processing_weight             as      processing_weight
        into    #barb_inds_with_sky
        from
                                        skybarb                 as      h
                inner join      barb_weights    as      w       on      h.house_id      =       w.household_number
                                                                                        and     h.person        =       w.person_number
        COMMIT -- (^_^)

        create hg index ind1 on #barb_inds_with_sky(household_number) COMMIT -- (^_^)


        -- nv_hhd_piv
        select          distinct a.hhsize
                   --     ,a.gender
                   --     ,a.ageband
                        ,case when b.household_number is not null then 'Viewing_HHD' else 'NonViewing_HHD' end as nv_hhd_status
                        ,sum(processing_weight) over (partition by a.hhsize, nv_hhd_status) as wieght
                        ,sum(processing_weight) over (partition by a.hhsize) as hh_size_wieght
                        ,wieght / cast(hh_size_wieght as float) as piv
        into            #nv_hhd_piv
        from            #barb_inds_with_sky a
        left join       #viewing_hhds b         on a.household_number = b.household_number
        --group by        a.hhsize
                    --    ,a.gender
                    --    ,a.ageband
                    --    ,nv_hhd_status

        COMMIT -- (^_^)

        create hg index ind1 on #nv_hhd_piv(hhsize) COMMIT -- (^_^)


        MESSAGE cast(now() as timestamp)||' | Begining M19.1 - NV viewing hhd profile from Barb - DONE' TO CLIENT

--------------------------------------
-- M19.2 - Non Viewing Household Profile From Barb
--------------------------------------

        MESSAGE cast(now() as timestamp)||' | Begining M19.2 - Assign NV HHDs' TO CLIENT

               -- Refresh nonviewer_household flag and set to 0 for all
                update          V289_M08_SKY_HH_COMPOSITION
                set             nonviewer_household = 0

                COMMIT -- (^_^)

                -- Set random number
                update          V289_M08_SKY_HH_COMPOSITION
                set             randd = RAND(exp_cb_key_db_person + DATEPART(us, GETDATE()))

                COMMIT -- (^_^)



                -- Select hhds from M08 to be Non Viewing Households.
                select          m08.account_number
                into            #nv_hhds
                from            V289_M08_SKY_HH_COMPOSITION m08
                inner join      #nv_hhd_piv nv on m08.household_size = nv.hhsize
                where           m08.panel_flag = 0
                                and m08.person_head = '1' -- to represent the hhd
                                and nv_hhd_status = 'NonViewing_HHD'
                                and m08.randd <= nv.piv

                COMMIT -- (^_^)

                create hg index ind1 on #nv_hhds(account_number) COMMIT -- (^_^)


                -- Select hhds from M08 to be Non Viewing Households. First restrict to head of hhd
                update          V289_M08_SKY_HH_COMPOSITION m08
                set             nonviewer_household = 1
                from            #nv_hhds nv
                where           m08.account_number = nv.account_number

                COMMIT -- (^_^)



        MESSAGE cast(now() as timestamp)||' | Begining M19.2 - Assign NV HHDs - DONE' TO CLIENT

END;                    --      END of Proc


COMMIT;
--GRANT EXECUTE   ON v289_m08_Experian_data_preparation   TO vespa_group_low_security;
--COMMIT;




