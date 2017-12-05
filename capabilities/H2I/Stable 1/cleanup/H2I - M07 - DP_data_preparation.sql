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

        This Module goal is to generate the probability matrices from BARB data to be used for identifying
        the most likely candidate(s) of been watching TV at a given event...

**Module:

        M07: DP Data Preparation
                        M07.0 - Initialising Environment
                        M07.1 - Compacting Data at Event level
                        M07.2 - Appending Dimensions
                        M07.3 - Assembling batches of overlaps
                        M07.4 - Returning Results

--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M07.0 - Initialising Environment
-----------------------------------

create or replace procedure v289_m07_dp_data_preparation
as begin

    MESSAGE cast(now() as timestamp)||' | Begining  M07.0 - Initialising Environment' TO CLIENT

        /*
                To prepare the DP data we first need to make sure we actually have something to prepare...
        */

        if      exists
                (
                        select  top 1 *
                        from    v289_M06_dp_raw_data
                )
        begin

                truncate table V289_M07_dp_data
                commit

                MESSAGE cast(now() as timestamp)||' | @ M07.0: Initialising Environment DONE' TO CLIENT

-----------------------------------------
-- M07.1 - Compacting Data at Event level
-----------------------------------------

                MESSAGE cast(now() as timestamp)||' | Begining  M07.1 - Compacting Data at Event level' TO CLIENT

                if exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('v289_m07_dp_data_tempshelf')
                        and     tabletype = 'TABLE')
                        drop table v289_m07_dp_data_tempshelf

                commit

                select  *
                into    v289_m07_dp_data_tempshelf
                from    v289_m07_dp_data

                commit

                insert  into v289_m07_dp_data_tempshelf (
                                                                                                        account_number
                                                                                                        ,subscriber_id
                                                                                                        ,event_id
                                                                                                        ,event_start_utc
                                                                                                        ,event_end_utc
                                                                                                        ,event_start_dim
                                                                                                        ,event_end_dim
                                                                                                        ,event_duration_seg
                                                                                                        ,programme_genre
                                                                                                )
                select  base.*
                                ,lookup.genre_description       as programme_genre -- Taking advantage here of getting this dimension already in place...
                from    (
                                        select  account_number
                                                        ,subscriber_id
                                                        ,min(pk_viewing_prog_instance_fact)         as event_id
                                                        ,event_start_date_time_utc                              as event_start_utc
                                                        ,case   when min(capping_end_Date_time_utc) is not null then min(capping_end_Date_time_utc)
                                                                        else event_end_date_time_utc
                                                        end     as event_end_utc
                                                        ,min(dk_event_start_datehour_dim)           as dk_event_start_dim
                                                        ,min(dk_event_end_datehour_dim)             as dk_event_end_dim
                                                        ,datediff(ss,event_start_utc,event_end_utc) as duration
                                        from    v289_M06_dp_raw_data
                                        group   by  account_number
                                                                ,subscriber_id
                                                                ,event_start_date_time_utc
                                                                ,event_end_date_time_utc
                                )   as base
                                inner join v289_M06_dp_raw_data as lookup
                                on  base.event_id   =   lookup.pk_viewing_prog_instance_fact

                commit

                create hg index hg1 on  v289_m07_dp_data_tempshelf(account_number)
                create hg index hg2 on  v289_m07_dp_data_tempshelf(subscriber_id)
                create hg index hg3 on  v289_m07_dp_data_tempshelf(event_id)
                create dttm index dttm1 on      v289_m07_dp_data_tempshelf(event_start_utc)
                create dttm index dttm2 on      v289_m07_dp_data_tempshelf(event_end_utc)
                commit

                MESSAGE cast(now() as timestamp)||' | @ M07.1: Compacting Data at Event level DONE' TO CLIENT

-------------------------------
-- M07.2 - Appending Dimensions
-------------------------------
                /*
                        All these dimensions appended in this part are those needed for the matrices...
                        They are currently 4:

                        + session_daypart       [DONE]
                        + hhsize                        [DONE]
                        + channel_pack          [DONE]
                        + programme_genre --> Appended on above section (M07.1)
                        + segment_id            [DONE]
                        + PIV Segment Consolidation

                */

                MESSAGE cast(now() as timestamp)||' | Begining  M07.2 - Appending Dimensions' TO CLIENT

                -- Session_daypart

                update  v289_m07_dp_data_tempshelf
                set     session_daypart =   case    when cast(event_start_utc as time) between '00:00:00.000' and '05:59:59.000' then 'night'
                                                                                        when cast(event_start_utc as time) between '06:00:00.000' and '08:59:59.000' then 'breakfast'
                                                                                        when cast(event_start_utc as time) between '09:00:00.000' and '11:59:59.000' then 'morning'
                                                                                        when cast(event_start_utc as time) between '12:00:00.000' and '14:59:59.000' then 'lunch'
                                                                                        when cast(event_start_utc as time) between '15:00:00.000' and '17:59:59.000' then 'early prime'
                                                                                        when cast(event_start_utc as time) between '18:00:00.000' and '20:59:59.000' then 'prime'
                                                                                        when cast(event_start_utc as time) between '21:00:00.000' and '23:59:59.000' then 'late night'
                                                                        end

                commit

                MESSAGE cast(now() as timestamp)||' | @ M07.2: Appending Session_Daypart DONE' TO CLIENT

                -- Channel_pack

                update  v289_m07_dp_data_tempshelf                                          as dpdata
                set     channel_pack    = cm.channel_pack
                from    v289_M06_dp_raw_data                                                    as dpraw
                                inner join vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES   as cm
                                on  dpraw.service_key   = cm.service_key
                where   dpraw.pk_viewing_prog_instance_fact = dpdata.event_id

                commit

                MESSAGE cast(now() as timestamp)||' | @ M07.2: Appending Channel_Pack DONE' TO CLIENT


                -- hhsize
                /*
                        Due the size of the HH Composition table we need to treat this separately
                        and store the result into a temp table to move on
                */

        update  v289_m07_dp_data_tempshelf    dpdata
                set     hhsize = base.household_size
                from    V289_M08_SKY_HH_composition as base
                where   base.account_number = dpdata.account_number

                commit

                MESSAGE cast(now() as timestamp)||' | @ M07.2: Appending HHSize DONE' TO CLIENT


                -- Segment_id

                update  v289_m07_dp_data_tempshelf    as dpdata
                set     dpdata.segment_id   = seglookup.segment_id
                from    V289_PIV_Grouped_Segments_desc   as seglookup
                where   seglookup.daypart       = dpdata.session_daypart
                and     seglookup.genre         = dpdata.programme_genre
                and     seglookup.channel_pack  = dpdata.channel_pack

                commit

                MESSAGE cast(now() as timestamp)||' | @ M07.2: Appending Segment_ID DONE' TO CLIENT

                -- PIV Segment Consolidation

                /*
                        03/11/2014

                        This is a new feature introduced because we want to do a segment comparison as a like for like between Vespa Events sample
                        and Barb for only what available on the day been processed...

                        Before we were using the default matrices to fill in any gaps for segments existing in vespa but not in barb for the
                        date in question... however this introduces households viewing from even segments that are not existing in the
                        processing date
                */
                /*
                MESSAGE cast(now() as timestamp)||' | @ M07.2: Trimming Vespa Sample to Only Segments on Barb for the processing date' TO CLIENT


                -- Generating a list of Segment_IDs existing in Vespa but not in barb to then be filtered out as mentioned above...
                select  m07.segment_id
                into    #segments_to_be_filtered
                from    (       -- listing all distinct segment_id from m07
                                        select  distinct segment_id
                                        from    v289_m07_dp_data_tempshelf
                                        where   segment_id is not null
                                )   as m07
                                left join   (   -- listing all distinct segment_id from session_size matrix
                                                                select  distinct segment_id
                                                                from    v289_sessionsize_matrix
                                                                where   thedate = (select min(cast(event_start_utc as date)) as thedate from v289_m07_dp_data_tempshelf)
                                                        )   as ses
                                on  m07.segment_id  = ses.segment_id
                                left join   (   -- listing all distinct segment_id from gender/age matrix
                                                                select  distinct segment_id
                                                                from    v289_genderage_matrix
                                                                where   thedate = (select min(cast(event_start_utc as date)) as thedate from v289_m07_dp_data_tempshelf)
                                                        )   as gen
                                on  m07.segment_id  = gen.segment_id
                where   (gen.segment_id is null or ses.segment_id is null)

                commit

                -- Trimming Vespa Sample...
                delete  from v289_m07_dp_data_tempshelf as m07
                from    #segments_to_be_filtered as filter
                where   filter.segment_id = m07.segment_id

                commit

                MESSAGE cast(now() as timestamp)||' | @ M07.2: Trimming Vespa Sample DONE' TO CLIENT
                */
                MESSAGE cast(now() as timestamp)||' | @ M07.2: Appending Dimensions DONE' TO CLIENT

-----------------------------------------
-- M07.3 - Assembling batches of overlaps
-----------------------------------------


                MESSAGE cast(now() as timestamp)||' | Begining  M07.3 - Flagging Overlapping Events' TO CLIENT

                -- Finding events that are overlapping with each others for each account

				
                if exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('v289_m07_events_overlap')
                        and     tabletype = 'TABLE')
                drop table v289_m07_events_overlap

                commit

                select  side_a.*
                                ,side_b.event_start_utc as event_start_b
                                ,side_b.event_end_utc   as event_end_b
                                ,dense_rank() over  (
                                                                                partition by    side_a.account_number
                                                                                order by        side_a.event_id
                                                                        )   as event_index
                into    v289_m07_events_overlap
                from    (
                                        select  account_number
                                                        ,subscriber_id
                                                        ,event_id
                                                        ,event_start_utc
                                                        ,event_end_utc
                                        from    v289_m07_dp_data_tempshelf
                                )   side_A
                                inner join  (
                                                                select  account_number
                                                                                ,event_id
                                                                                ,event_start_utc
                                                                                ,event_end_utc
                                                                from    v289_m07_dp_data_tempshelf
                                                        )   as side_b
                                on  side_a.account_number   = side_b.account_number
                                and (
                                                (side_a.event_start_utc >       side_b.event_Start_utc and side_a.event_start_utc       <       side_b.event_end_utc)
                        or
                        (side_a.event_end_utc   >       side_b.event_Start_utc and side_a.event_end_utc         <       side_b.event_end_utc)
                        or
                        (side_b.event_Start_utc >       side_a.event_start_utc and side_b.event_Start_utc       <       side_a.event_end_utc)
                        or
                        (side_b.event_end_utc   >       side_a.event_Start_utc and side_b.event_end_utc         <       side_a.event_end_utc)
                    )


                commit

                create hg index hg1     on v289_m07_events_overlap(account_number)
                create hg index hg2             on v289_m07_events_overlap(subscriber_id)
                create hg index hg3             on v289_m07_events_overlap(event_id)
                create dttm index dttm1 on v289_m07_events_overlap(event_start_utc)
                create dttm index dttm2 on v289_m07_events_overlap(event_end_utc)
                create dttm index dttm3 on v289_m07_events_overlap(event_start_b)
                create dttm index dttm4 on v289_m07_events_overlap(event_end_b)
                commit

                grant select on v289_m07_events_overlap to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M07.3: Flagging Overlapping Events DONE' TO CLIENT

                -- breaking overlapping events into chunks
                /*
                        this bit stack all start and end dates on top of each other for each event coming from an account
                        with the idea of setting the timeline for the chunks, overlapping timestamps are sorted ascendantly
                        and identifying where the event starts (theflag = 1) then we can easly create the chunks by lead/lagging
                        the dates...
                */
                if  exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('v289_m07_overlaps_chunks')
                        and     tabletype = 'TABLE')
                        drop table v289_m07_overlaps_chunks

                commit

                select  *
                                ,min(chunk_start) over  (
                                                                                        partition by    account_number
                                                                                                                        ,event_id
                                                                                        order by        chunk_start
                                                                                        rows between    1 following and 1 following
                                                                                )   as chunk_end
                into    v289_m07_overlaps_chunks
                from    (
                                        select  distinct *
                                        from    (
                                                                select  account_number
                                                                                ,event_id
                                                                                ,event_start_utc        as chunk_start
                                                                                ,event_index
                                                                                ,1 as theflag
                                                                from    v289_m07_events_overlap
                                                                union   all
                                                                select  account_number
                                                                                ,event_id
                                                                                ,event_end_utc          as chunk_start
                                                                                ,event_index
                                                                                ,0 as theflag
                                                                from    v289_m07_events_overlap
                                                                union   all
                                                                select  account_number
                                                                                ,event_id
                                                                                ,event_start_b          as chunk_start
                                                                                ,event_index
                                                                                ,0 as theflag
                                                                from    v289_m07_events_overlap
                                                                where   event_start_b > event_start_utc
                                                                union   all
                                                                select  account_number
                                                                                ,event_id
                                                                                ,event_end_b            as chunk_start
                                                                                ,event_index
                                                                                ,0 as theflag
                                                                from    v289_m07_events_overlap
                                                                WHERE event_end_b <= event_end_utc
                                                        )   as base
                                )   as base2
                commit

                create hg index hg1             on v289_m07_overlaps_chunks(account_number)
                create hg index hg2     on v289_m07_overlaps_chunks(event_id)
                create dttm index dttm1 on v289_m07_overlaps_chunks(chunk_start)
                create dttm index dttm2 on v289_m07_overlaps_chunks(chunk_end)
                commit

                grant select on v289_m07_overlaps_chunks to vespa_group_low_security
                commit

                drop table v289_m07_events_overlap
                commit

                MESSAGE cast(now() as timestamp)||' | @ M07.3: Breaking Overlapping Events into Chunks DONE' TO CLIENT

                -- Identifying batches of overlaps for each account
                /*
                        chunks of events starting/ending at the same time overlap, hence they all get wrapped up into a single
                        batch... batches can also be made of 1 single chunk and that means is the head,body or tail of an event
                        that is not overlapping with others
                */
                if  exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('v289_m07_overlap_batches')
                        and     tabletype = 'TABLE')
                        drop table v289_m07_overlap_batches

                commit

                select  side_a.*
                                ,dense_rank() over  (
                                                                                partition by    side_a.account_number
                                                                                order by        side_a.chunk_start
                                                                        )   as thebatch
                into    v289_m07_overlap_batches
                from    v289_m07_overlaps_chunks    as side_a
                                inner join  (
                                                                select  distinct
                                                                                account_number
                                                                                ,event_id
                                                                                ,chunk_start
                                                                from    v289_m07_overlaps_chunks
                                                                where   theflag = 1
                                                        )   as side_b
                                on  side_a.account_number    = side_b.account_number
                                and side_a.event_id          = side_b.event_id
                where   side_a.chunk_end is not null
                and     side_b.chunk_start <= side_a.chunk_start
                and     side_a.chunk_start <> side_a.chunk_end

                commit

                create hg index hg1     on v289_m07_overlap_batches(account_number)
                create hg index hg2 on v289_m07_overlap_batches(event_id)
                create dttm index dttm1 on v289_m07_overlap_batches(chunk_start)
                create dttm index dttm2 on v289_m07_overlap_batches(chunk_end)
                create lf index lf1             on v289_m07_overlap_batches(thebatch)
                commit

                grant select on v289_m07_overlap_batches to vespa_group_low_security
                commit

                drop table v289_m07_overlaps_chunks
                commit

                MESSAGE cast(now() as timestamp)||' | @ M07.3: Assembling batches of overlaps DONE' TO CLIENT


----------------------------
-- M07.4 - Returning Results
----------------------------

                MESSAGE cast(now() as timestamp)||' | Begining  M07.4 - Returning Results' TO CLIENT

                insert  into V289_M07_dp_data
                select  dpdata.account_number
                                ,dpdata.subscriber_id
                                ,dpdata.event_id
                                ,dpdata.event_start_utc
                                ,dpdata.event_end_utc
                                ,overlap.chunk_start
                                ,overlap.Chunk_end
                                ,dpdata.event_duration_seg
                                ,case   when overlap.chunk_start is not null then datediff(second,overlap.chunk_start,overlap.chunk_end)
                                                else null
                                end     as chunk_duration_seg
                                ,dpdata.programme_genre
                                ,dpdata.session_daypart
                                ,dpdata.hhsize
                                ,0 -- viewer_hhdsize
                                ,dpdata.channel_pack
                                ,dpdata.segment_id
                                ,overlap.thebatch
                                ,0                      as session_size
                                ,dpdata.event_start_dim
                                ,dpdata.event_end_dim
                from    v289_m07_dp_data_tempshelf          as dpdata
                                left join v289_m07_overlap_batches  as overlap
                                on  dpdata.account_number   = overlap.account_number
                                and dpdata.event_id         = overlap.event_id

                commit

                drop table v289_m07_dp_data_tempshelf
                commit

                MESSAGE cast(now() as timestamp)||' | @ M07.4: Output table V289_M07_DP_DATA DONE' TO CLIENT

        end
        else
        begin

                MESSAGE cast(now() as timestamp)||' | @ M07.0: Missing DP Viewing Data to prepare( v289_M06_dp_raw_data empty)!!!' TO CLIENT

        end

        MESSAGE cast(now() as timestamp)||' | M07 Finished' TO CLIENT

end;

commit;
grant execute on v289_m07_dp_data_preparation to vespa_group_low_security;
commit;
