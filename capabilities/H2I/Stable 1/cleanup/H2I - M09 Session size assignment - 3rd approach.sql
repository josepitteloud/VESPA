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

        This Module goal is to assign a session size to all the events using Monte Carlo simulation process. 

**Module:
        
        M09: Session Size Assignment process
                        M09.0 - Initialising Environment
                        M09.1 - Creating transient tables 
                        M09.2 - Single box events update
                        M09.3 - Multi Box events update
                        M09.4 - Main event tables update
        
--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M09.0 - Initialising Environment
-----------------------------------

create or replace procedure v289_m09_Session_size_definition
AS BEGIN

        MESSAGE cast(now() as timestamp)||' | Begining M09.0 - Initialising Environment' TO CLIENT
                


        -----------------       Session VARIABLEs Definition
        DECLARE @account     varchar(20)     ---Account Number
        DECLARE @subs        decimal(10)     ---Subscriber ID
        DECLARE @iter        tinyint         ---Max iteration x accounts
        DECLARE @cont        tinyint         ---counter for subs iteration
        DECLARE @event       bigint          ---for event iteration
        DECLARE @length      DECIMAL(7,6)    ---MC proportional length
        DECLARE @random      FLOAT           ---MC Random number
        DECLARE @s_size      tinyint         ---Event session size
        DECLARE @adj_hh      tinyint         ---Adjusted HH size (for MC Multibox process only)
        DECLARE @hh_size     tinyint         ---HH size
        DECLARE @segment     tinyint         ---Segment_ID
        DECLARE @batch       tinyint         ---Overlap Batch
        DECLARE @row_id          INT
        DECLARE @event_id    BIGINT
        DECLARE @maxi       tinyint
        commit


        ---------------------------------------
        -- M09.1 - Creating transient tables
        ---------------------------------------


        -----------------       temp_event Table Creation
        
        SELECT
              count (event_id)  AS overlap_size
            , account_number
            , Overlap_batch
    into        #tmp1
    FROM V289_M07_dp_data
    GROUP BY Overlap_batch, account_number
    commit
        
    create hg index tmp1_idx_1 on #tmp1(account_number)
    create lf index tmp1_idx_2 on #tmp1(overlap_batch)
        commit

                if  exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('temp_event')
                        and     tabletype = 'TABLE')        
                DROP TABLE temp_event
        SELECT
                        event_ID
                        , dt.account_number
                        , dt.subscriber_id
                        , CAST(event_start_utc AS DATE) event_dt
                        , CASE WHEN hhsize > viewer_hhsize      THEN viewer_hhsize      ELSE hhsize             END AS new_hh_size
                        , CASE WHEN new_hh_size > 8             THEN 8                          ELSE new_hh_size        END as hhsize_
                        , COALESCE (dt.segment_ID, 157) AS segment_ID
                        , random1       =   RAND(dt.event_id + DATEPART(us, GETDATE()))
                        , overlap       =   ov.overlap_size
                        , COALESCE(dt.overlap_batch,0)  AS overlap_batch
                        , box_rank      =   dense_rank() OVER (PARTITION BY dt.account_number, dt.Overlap_batch ORDER BY subscriber_id, event_end_utc  DESC)
                        , CAST(0 AS tinyint) session_size
        INTO temp_event
        FROM
                                        V289_M07_dp_data        AS      dt
                LEFT JOIN       #tmp1                           AS      ov      ON      ov.account_number = dt.account_number
                                                                                                AND     ov.Overlap_batch = dt.overlap_batch
        WHERE hhsize_ > 0 and session_size = 0

        MESSAGE cast(now() as timestamp)||' | @ M09.1: temp_Event Table created: '||@@rowcount TO CLIENT

        COMMIT

        CREATE HG INDEX ide1 ON temp_event(event_ID)
        CREATE LF INDEX ide2 ON temp_event(overlap_batch)
        CREATE LF INDEX ide3 ON temp_event(segment_ID)
        CREATE LF INDEX ide4 ON temp_event(hhsize_)
        COMMIT
        
        -- cleanup
        drop table #tmp1
        commit




        ------------------------------
        -- M09.2 - Single box events update
        ------------------------------

        UPDATE temp_event
        SET ev.session_size = COALESCE(sm.session_size, mx.session_size)
        FROM temp_event as ev
        LEFT JOIN v289_sessionsize_matrix                       AS sm ON  sm.segment_ID = ev.segment_id 
                                                                                                                        AND ev.hhsize_  =       sm.hhsize 
                                                                                                                        AND random1     >   sm.lower_limit 
                                                                                                                        AND random1     <=  sm.upper_limit
                                                                                                                        AND ev.event_dt =       sm.thedate
        JOIN v289_sessionsize_matrix_default    AS mx ON  mx.segment_ID = ev.segment_id 
                                                                                                                        AND ev.hhsize_  =       mx.hhsize 
                                                                                                                        AND random1     >       mx.lower_limit 
                                                                                                                        AND random1     <=      mx.upper_limit
        WHERE Overlap_batch = 0 OR overlap = 1


        MESSAGE cast(now() as timestamp)||' | @ M09.2: Single Box events done: '||@@rowcount TO CLIENT

        
        
        --------------------------
        -- M09.3: Multi Box events
        --------------------------
        
        -----------------   MULTI box events update
        -----------------   Primary box processing
        MESSAGE cast(now() as timestamp)||' | @ M09.3: Multi Box events started '||@@rowcount TO CLIENT

                if exists(  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('events_1_box')
                        and     tabletype = 'TABLE')        
        DROP TABLE events_1_box
        SELECT
          *
        , row_id        = row_number() over(order by subscriber_id)
        , ev_proc_flag  =   CAST (0 AS BIT)
        , adj_hh        = hhsize_ - overlap + 1
        , length_1      = CAST (0 as DECIMAL (7,6))
        INTO events_1_box
        FROM temp_event
        WHERE       session_size = 0
                        AND hhsize_ is not null
                        AND box_rank = 1
        ORDER BY account_number, subscriber_id, overlap_batch
        
        MESSAGE cast(now() as timestamp)||' | @ M09.3: Multi Box primary box table populated: '||@@rowcount TO CLIENT
        
        COMMIT
        CREATE HG       INDEX idxe1     ON events_1_box (event_ID)
        CREATE LF       INDEX id1       ON events_1_box (overlap_batch)
        CREATE HG       INDEX id2       ON events_1_box (subscriber_id)
        CREATE LF       INDEX box       ON events_1_box (box_rank)
        CREATE HG       INDEX box1      ON events_1_box (length_1)
        CREATE LF       INDEX box2      ON events_1_box (adj_hh)

        COMMIT
        
        UPDATE events_1_box
    SET adj_hh = 1
    WHERE adj_hh < 1
        

        UPDATE events_1_box
        SET     length_1 = upper_limit
                        , random1 = random1 * upper_limit
        FROM events_1_box AS ev
        JOIN v289_sessionsize_matrix_default as mx ON mx.segment_ID = ev.segment_id
                                                                                                                                        AND ev.hhsize_ = mx.hhsize
                                                                                                                                        AND ev.adj_hh = mx.session_size

        COMMIT
        
        -- Separated version of the above 4-table join:
        select
                        ev.event_ID
                ,       ev.overlap_batch
                ,       ev.segment_id
                ,       ev.hhsize_
                ,       ev.event_dt
                ,       ev1.adj_hh
                ,       ev1.random1
        into    #tmp1
        FROM
                                        temp_event              as      ev
                INNER JOIN      events_1_box    as      ev1             ON              ev.event_ID = ev1.event_ID
                                                                                                AND     ev.overlap_batch = ev1.overlap_batch
        commit
        
        create hg index tmp1_idx_1 on #tmp1(event_ID)
        create lf index tmp1_idx_2 on #tmp1(overlap_batch)
        commit
        
        
        select
                        tmp.*
                ,       mx.session_size         as      mx_session_size
        into    #tmp2
        from
                                        #tmp1                                                           as      tmp
                INNER JOIN      v289_sessionsize_matrix_default         AS      mx      ON  mx.segment_ID = tmp.segment_id
                                                                                                                                AND tmp.hhsize_ = mx.hhsize
                                                                                                                                AND tmp.random1 >  mx.lower_limit
                                                                                                                                AND tmp.random1 <= mx.upper_limit
        commit
        
        create hg index tmp2_idx_1 on #tmp2(event_ID)
        create lf index tmp2_idx_2 on #tmp2(overlap_batch)
        commit
        
        
        select
                        tmp.*
                ,       sm.session_size         as      sm_session_size
                ,       COALESCE(sm_session_size, mx_session_size)      as      ev_session_size
        into    #tmp3
        from
                                        #tmp2                                           as      tmp
                left join       v289_sessionsize_matrix         as      sm              ON  sm.segment_ID = tmp.segment_id 
                                                                                                                        AND tmp.hhsize_         =       sm.hhsize
                                                                                                                        AND tmp.adj_hh  >=      sm.session_size
                                                                                                                        AND tmp.random1         >   sm.lower_limit 
                                                                                                                        AND tmp.random1         <=  sm.upper_limit
                                                                                                                        AND tmp.event_dt =      sm.thedate
        commit
        
        create hg index tmp3_idx_1 on #tmp3(event_ID)
        create lf index tmp3_idx_2 on #tmp3(overlap_batch)
        commit
        
        
        update temp_event
        SET ev.session_size =  tmp.ev_session_size
        FROM 
                                        temp_event      as      ev
                inner join      #tmp3           as      tmp             on              ev.event_ID = tmp.event_ID
                                                                                        and             ev.overlap_batch = tmp.overlap_batch
                                                                                        and             ev.segment_id = tmp.segment_id
                                                                                        and             ev.hhsize_ = tmp.hhsize_
                                                                                        and             ev.event_dt = tmp.event_dt

        -- Clean up
        drop table #tmp1
        drop table #tmp2
        drop table #tmp3
        commit


                                                                                        
        MESSAGE cast(now() as timestamp)||' | @ M09.3: Multi Box primary box events updated: '||@@rowcount TO CLIENT
        
        COMMIT
        


        -----------------   Secondary box processing - Other Boxes
        MESSAGE cast(now() as timestamp)||' | @ M09.4: Multi Box Other boxes loop started ' TO CLIENT
        SET @cont = 2
        SET @maxi = (SELECT MAX(box_rank)+1 FROM temp_event WHERE overlap is not null)
        SET @maxi = CASE WHEN @maxi > 15 THEN 15 ELSE @maxi END
        
        WHILE @cont <= @maxi
        BEGIN
                
                MESSAGE cast(now() as timestamp)||' | @ M09.4: Multi Box start box #: '||@cont TO CLIENT
                
                -- This table is reused from above
                if exists (  select tname from syscatalog
                        where creator = user_name()
                        and upper(tname) = upper('events_1_box')
                        and     tabletype = 'TABLE')
                        truncate TABLE events_1_box
                
                commit
                
                SELECT  Overlap_batch
                                ,account_number
                                ,SUM (session_size) s_size
                                ,COUNT (subscriber_id) boxes
                into    #tmp1
                FROM    temp_event
                GROUP   BY      Overlap_batch
                                        ,account_number
                commit
                
                create hg index tmp1_idx_1 on #tmp1(account_number)
                create lf index tmp2_idx_2 on #tmp1(overlap_batch)
                commit

                insert  INTO events_1_box
                SELECT  te.*
                                ,row_id        = row_number() over(order by subscriber_id)
                                ,ev_proc_flag  = CAST (0 AS BIT)
                                ,adj_hh        = te.hhsize_ - v.s_size -(v.boxes -@cont)
                                ,length_1      = CAST (0 as DECIMAL (7,6))
                FROM 
                                                temp_event      as te
                        INNER JOIN      #tmp1           AS v    ON      v.Overlap_batch = te.Overlap_batch  
                                                                                        AND v.account_number = te.account_number
                WHERE   te.session_size = 0
                AND     te.hhsize_ is not null
                AND     te.box_rank = @cont
                ORDER   BY      te.account_number
                                        ,te.subscriber_id
                                        ,te.overlap_batch       
                
                commit
                
                -- cleanup
                drop table #tmp1
                commit
                
                MESSAGE cast(now() as timestamp)||' | @ M09.4: Multi Box events_1_box table populated: '||@@rowcount TO CLIENT
                
                UPDATE events_1_box
                SET adj_hh = 1
                WHERE adj_hh < 1
                
                UPDATE events_1_box
                SET     length_1 = upper_limit
                                , random1 = random1 * upper_limit
                FROM events_1_box AS ev
                JOIN v289_sessionsize_matrix_default as mx ON mx.segment_ID = ev.segment_id
                                                                                                                                                AND ev.hhsize_ = mx.hhsize
                                                                                                                                                AND ev.adj_hh = mx.session_size
                
                
                -- Separated version of the above 4-table join:
                select
                                ev.event_ID
                        ,       ev.overlap_batch
                        ,       ev.segment_id
                        ,       ev.hhsize_
                        ,       ev.event_dt
                        ,       ev1.adj_hh
                        ,       ev1.random1
                into    #tmp1
                FROM
                                                temp_event              as      ev
                        INNER JOIN      events_1_box    as      ev1             ON              ev.event_ID = ev1.event_ID
                                                                                                        AND     ev.overlap_batch = ev1.overlap_batch
                commit
                
                create hg index tmp1_idx_1 on #tmp1(event_ID)
                create lf index tmp1_idx_2 on #tmp1(overlap_batch)
                commit
                
                
                select
                                tmp.*
                        ,       mx.session_size         as      mx_session_size
                into    #tmp2
                from
                                                #tmp1                                                           as      tmp
                        INNER JOIN      v289_sessionsize_matrix_default         AS      mx      ON  mx.segment_ID = tmp.segment_id
                                                                                                                                        AND tmp.hhsize_ = mx.hhsize
                                                                                                                                        AND tmp.random1 >  mx.lower_limit
                                                                                                                                        AND tmp.random1 <= mx.upper_limit
                commit
                
                create hg index tmp2_idx_1 on #tmp2(event_ID)
                create lf index tmp2_idx_2 on #tmp2(overlap_batch)
                commit
                
                
                select
                                tmp.*
                        ,       sm.session_size         as      sm_session_size
                        ,       COALESCE(sm_session_size, mx_session_size)      as      ev_session_size
                into    #tmp3
                from
                                                #tmp2                                           as      tmp
                        left join       v289_sessionsize_matrix         as      sm              ON  sm.segment_ID = tmp.segment_id 
                                                                                                                                AND tmp.hhsize_         =       sm.hhsize 
                                                                                                                                AND tmp.adj_hh  >=      sm.session_size
                                                                                                                                AND tmp.random1         >   sm.lower_limit 
                                                                                                                                AND tmp.random1         <=  sm.upper_limit
                                                                                                                                AND tmp.event_dt =      sm.thedate
                commit

                create hg index tmp3_idx_1 on #tmp3(event_ID)
                create lf index tmp3_idx_2 on #tmp3(overlap_batch)
                commit
                
                
                update temp_event
                SET ev.session_size =  tmp.ev_session_size
                FROM 
                                                temp_event      as      ev
                        inner join      #tmp3           as      tmp             on              ev.event_ID = tmp.event_ID
                                                                                                and             ev.overlap_batch = tmp.overlap_batch
                                                                                                and             ev.segment_id = tmp.segment_id
                                                                                                and             ev.hhsize_ = tmp.hhsize_
                                                                                                and             ev.event_dt = tmp.event_dt

                -- Clean up
                drop table #tmp1
                drop table #tmp2
                drop table #tmp3
                commit


                MESSAGE cast(now() as timestamp)||' | @ M09.3: Multi Box box#: '||@cont||'  events updated: '||@@rowcount TO CLIENT
            COMMIT

                SET @cont = @cont +1
        END
        IF (SELECT count(1) FROM temp_event WHERE session_size = 0) <> 0 
        MESSAGE CAST(now() as timestamp)||' | Event assignemnet INCOMPLETE!!!'  TO CLIENT
        
        ------------------------------
        -- M09.4 - Main event tables update
        ------------------------------
        
        UPDATE V289_M07_dp_data
        SET dt.session_size = te.session_size
        FROM V289_M07_dp_data AS dt
        INNER JOIN temp_event AS te ON te.event_id = dt.event_id AND te.overlap_batch = dt.overlap_batch
        
        MESSAGE cast(now() as timestamp)||' | @ M09.4: Multi Box events updated: '||@@rowcount TO CLIENT
        
        UPDATE V289_M07_dp_data
        SET dt.session_size = te.session_size
        FROM V289_M07_dp_data AS dt
        INNER JOIN temp_event AS te ON te.event_id = dt.event_id 
        WHERE te.overlap_batch = 0
        
        MESSAGE cast(now() as timestamp)||' | @ M09.4: Single Box events updated: '||@@rowcount TO CLIENT
        
        --DROP TABLE temp_event

        COMMIT

        

END;


COMMIT;
GRANT EXECUTE   ON v289_m09_Session_size_definition     TO vespa_group_low_security;
COMMIT



/*          QA
SELECT top 100 * FROM temp_event WHERE session_size is not null
SELECT top 100 * FROM accounts
SELECT top 100 * FROM events
SELECT top 100 * FROM MC_event
*/
