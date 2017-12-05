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

        This Module goal is to Flag Non-viewers households from the M08 Tables 

**Module:
        
        M19: Non-Viewers Household
                        M19.0 - Initialising Environment
                        M19.1 - Non Viewing Household Profile From Barb
                        M19.2 - Non Viewing Household Profile From Barb
       
	   
--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M19.0 - Initialising Environment
-----------------------------------

create or replace procedure ${SQLFILE_ARG001} .v289_M19_Non_Viewing_Households
        @processing_day date

AS BEGIN

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

                ,       count(1) over (partition by h.house_id) as hhsize
                ,       w.processing_weight             as      processing_weight
        into    #barb_inds_with_sky
        from
                                        skybarb                 as      h
                inner join      barb_weights    as      w       on      h.house_id      =       w.household_number
                                                                                        and     h.person        =       w.person_number
        COMMIT -- (^_^)

        create hg index ind1 on #barb_inds_with_sky(household_number) COMMIT -- (^_^)

        select          distinct a.hhsize

                        ,case when b.household_number is not null then 'Viewing_HHD' else 'NonViewing_HHD' end as nv_hhd_status
                        ,sum(processing_weight) over (partition by a.hhsize, nv_hhd_status) as wieght
                        ,sum(processing_weight) over (partition by a.hhsize) as hh_size_wieght
                        ,wieght / cast(hh_size_wieght as float) as piv
        into            #nv_hhd_piv
        from            #barb_inds_with_sky a
        left join       #viewing_hhds b         on a.household_number = b.household_number


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
                
--              first get counts of hhds in each hhd size in used vespa panel and hence establish number non viewers required from non-panel

                select         household_size, ceil(count(account_number)/(1-avg(nv.piv)))-count(account_number) as reqd_nvs
                into            #hhd_panel_counts
                from            V289_M08_SKY_HH_COMPOSITION m08
                inner join
                #nv_hhd_piv nv on m08.household_size = nv.hhsize
                where panel_flag=1
                and m08.person_head = '1' -- to represent the hhd
                and nv_hhd_status = 'NonViewing_HHD'
                group by household_size


-- rank the random number and choose the hhds whose ranking is less than or equal to required number.

    -- rank the random numbers

    SELECT hhc.account_number,household_size,rank() over( partition by household_size order by randd) as ranknum
    into #aclist
    FROM V289_M08_SKY_HH_composition hhc
    where panel_flag=0
    and hhc.person_head = '1'
    commit



                select          ac.account_number
                into            #nv_hhds
                from            #hhd_panel_counts hpc
                inner join      #aclist ac on hpc.household_size = ac.household_size
                where            ac.ranknum <= hpc.reqd_nvs


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
GO
COMMIT;

GRANT EXECUTE   ON v289_M19_Non_Viewing_Households   TO vespa_group_low_security;
COMMIT;




