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

-----------------------------------------------------------------------------------

**Project Name:                         Panel Balancing
**Analyst:                              Jonathan Green
**Contributions From:                   Hoi Yu Tang, Leonardo Ripoli, Jason Thompson, Jose Loureda
**Stakeholder:                          Jose Loureda
**Project Code (Insight Collation):     V306, V352
**SharePoint Folder:                    http://sp-department.bskyb.com/sites/IQSKY/SIG/Insight%20Collation%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FIQSKY%2FSIG%2FInsight%20Collation%20Documents%2F01%20Analysis%20Requests%2FV306%20-%20Foundation%20-%20Platform%20Maintenance%2FPhase%202%2FPanel%20Balancing

**Business Brief:

Panel Balancing is a regular exercise to ensure that the viewing panel is as
representative of the wider Sky customer base as possible as the latter evolves over
time. Balancing is also crucial in ensuring that key KPI and contractual obligations
of account coverage are maintained.




**Module:                              M07_VolCheck

This module adds additional accounts to the panels if any are required to meet the requirements for TA propensity coverage, virtual panel coverage, or business requirements for the viewing or marketing panels.
If the maximum size of the primary or combined panels has been exceeded, then the required number of accounts rae removed from the panels. In practice, this never happens.

*/


  create or replace procedure V352_M07_VolCheck
         @max_b      int --boxes to return per day
                                                                ,@min_vp1    int
                                                                ,@min_ta     float --turnaround propensity coverage
                                                                ,@min_ta_ret float --turnaround propendity from returning boxes
      as begin

            create table temp_lookup(
                   account_number                                     varchar(30) null
                  ,boxes                                              int null
                   )

            create table temp_list1_rq(
                   account_number                                     varchar(30) null
                  ,rq                                                 double null
                  ,boxes                                              int null
                   )

           declare @cow         int
           declare @precision   int
           declare @virtuals    int
           declare @ta          double
           declare @records_out int default 1
                                                                                        
               set @precision = 1000
               
                -- check whether we are over the limit for boxes dialling back per day
            select @cow = sum(case when rq is null then case when cbck_rate is null then 1 else cbck_rate end when rq > 1 then 1 else rq end) * boxes
              from PanBal_panel as bas
                   inner join panbal_sav as sav on bas.account_number = sav.account_number

                if (@cow > @max_b) begin

                       while (@cow > @max_b) begin
                              truncate table temp_lookup

                                   set rowcount @precision

                                          insert into temp_lookup
                                          select pan.account_number
                                                ,boxes * case when sav.rq is null then case when cbck_rate is null then 1 
                                                                                                                   else cbck_rate 
                                                                                        end 
                                                              when sav.rq > 1 then 1 
                                                                              else sav.rq 
                                                          end
                                            from panbal_panel as pan
                                                 inner join panbal_sav as sav on pan.account_number = sav.account_number
                                        order by sav.rq
                                                ,pan.account_number

                                   set rowcount 0

                                delete from panbal_panel where account_number in (select account_number from temp_lookup)

                                select @cow = sum(case when rq is null then case when cbck_rate is null then 1 else cbck_rate end when rq > 1 then 1 else rq end) * boxes
                                  from PanBal_panel as bas
                                       inner join panbal_sav as sav on bas.account_number = sav.account_number

                                insert into panbal_amends(account_number, movement)
                                select account_number
                                      ,'Account to remove from primary panels'
                                  from temp_lookup

                         end --while
               end --if
                                                                                                                                                                                                                                                
                -- now need to remove the ones we've added fom the secondary panel (we have already deleted the ones we've added from the waterfall pool)
            delete from secondary_panel_pool where account_number in (select account_number from PanBal_panel)
            delete from pstn_panel_pool where account_number in (select account_number from PanBal_panel)

                -- accounts to add to secondary panels, to make 50% in each segment (if poss)
            select sum(case when pan.account_number is null then 0 else 1 end)                                    as vespa
                  ,sum(case when alt.account_number is not null and pan.account_number is null then 1 else 0 end) as alt
                  ,bss.segment_id
              into temp_panels
              from PanBal_segment_snapshots                     as bss
                   left join PanBal_panel                       as pan on bss.account_number = pan.account_number
                   left join secondary_panel_pool               as alt on bss.account_number = alt.account_number
                   inner join panbal_segments_lookup_normalised as lkp on bss.segment_id = lkp.segment_id
             where aggregation_variable = 2
               and value not like 'ROI%'
          group by bss.segment_id

            select segment_id
                  ,vespa - (alt * 2) as reqd
              into temp_reqd
              from temp_panels
             where reqd > 0

            select wat.account_number
                  ,wat.segment_id
                  ,rank() over (partition by wat.segment_id order by cbck_rate desc, vp1 desc) as rnk
                  ,vp1
                  ,boxes
              into temp_available
              from waterfall_pool                          as wat
                   inner join panbal_sav                   as sav on wat.account_number = sav.account_number
                   left join PanBal_panel                  as bas on wat.account_number = bas.account_number
             where bas.account_number is null

            insert into panbal_amends(account_number, movement)
            select account_number
                  ,'Account to add to secondary panels as segment backup'
              from temp_available        as ava
                   inner join  temp_reqd as req on ava.segment_id = req.segment_id
             where rnk <= reqd

             delete from waterfall_pool
             where account_number in (select account_number 
                                        from panbal_amends
                                     )
                                     
             delete from pstn_waterfall_pool
             where account_number in (select account_number 
                                        from panbal_amends
                                     )
                                                                                                                                                                                                                
            select @virtuals = sum(boxes) --count boxes on the virtual panel on the new panel
              from PanBal_panel                            as bas
                   inner join panbal_sav                   as sav on bas.account_number = sav.account_number
             where vp1 = 1

            select @virtuals = @virtuals + sum(boxes) --add on the remaining accounts left in the secondary panel pool
              from secondary_panel_pool                                              as bas
                   inner join panbal_sav                   as sav on bas.account_number = sav.account_number
                   left join panbal_amends                 as ame on bas.account_number = ame.account_number
             where vp1 = 1
               and ame.account_number is null

                           select @virtuals = @virtuals + sum(boxes) --add on the remaining accounts left in the secondary panel pool
              from secondary_panel_pool                                              as bas
                   inner join panbal_sav                   as sav on bas.account_number = sav.account_number
                   left join panbal_amends                 as ame on bas.account_number = ame.account_number
             where vp1 = 1
               and ame.account_number is null

                -- we are still missing the secondary panel accounts with unacceptable reporting:
            select @virtuals = @virtuals + sum(boxes)
              from panbal_sav
             where panel not in (11, 12)
               and rq > 0
               and rq < @max_rq
               and vp1 = 1
               and segment_id is not null

                if (@virtuals < @min_vp1) begin --do we need any more on the channel 4 panel?

                    truncate table temp_list1_rq
                      insert into temp_list1_rq
                      select li4.account_number
                            ,case when sav.rq is null then case when cbck_rate is null then 1 else cbck_rate end when sav.rq > 1 then 1 else sav.rq end
                            ,boxes
                        from waterfall_pool                              as li4
                             inner join panbal_sav                       as sav on li4.account_number = sav.account_number
                       where vp1 = 1

                       while (@virtuals < @min_vp1 and @records_out > 0) begin
                              truncate table temp_lookup
                                                                                                                                                                                                                                                
                                select @records_out = count(1)
                                  from list1_rq
                                                                                                                                                                                                                                                
                                   set rowcount @precision

                                                                                                                                                                                                                                                                                                                                                insert into temp_lookup
                                                                                                                                                                                                                                                                                                                                                select account_number
                                                                                                                                                                                                                                                                                                                                                                                                ,boxes
                                                                                                                                                                                                                                                                                                                                                                from temp_list1_rq
                                                                                                                                                                                                                                                                                                                                order by rq
                                                                                                                                                                                                                                                                                                                                                                                                ,account_number

                                                                                                                                                                                                                                                                                        set rowcount 0
                                   set @virtuals = @virtuals + (select sum(boxes) from temp_lookup)

                                select @cow = count(1) from temp_list1_rq

                                    if (@cow = 0) begin

                                             set @cow = @min_vp1 - @virtuals

                                          insert into panbal_amends(account_number, movement)
                                          select null
                                                ,@cow || ' more boxes needed on the virtual panel'

                                             set @virtuals = @min_vp1
                                   end

                                delete from temp_list1_rq where account_number in (select account_number from temp_lookup)

                                insert into panbal_amends(account_number, movement)
                                select account_number
                                      ,'Account to add to secondary panels for virtual panel req.'
                                  from temp_lookup

                         end --while
                      commit

               end --if

                                                                                                                        if (@records_out = 0) begin
                                                                                                                                                                        insert into panbal_amends(account_number
                                                                                                                                                                                                                                                                                                                                                                                ,movement)
                                                                                                                                                                        select 0
                           ,'Not enough accounts available to fill the Virtual Panel 1 requirement'
                                                                                                                end
                                                                                                                                                                                                        
                -- check TA coverage - we need at least 25% from enabled accounts on all panels
            select @ta = sum(ta_propensity)
              from panbal_sav as sav
                   left join panbal_amends  as pan on sav.account_number = pan.account_number
             where panel is not null
                or pan.account_number is not null

            select @ta = @ta / sum(ta_propensity)  from vespa_analysts.SkyBase_TA_scores

             while (@ta < @min_ta and @records_out > 0) begin
                                                                                                        
                                                                                                                                                                                select @records_out = count(1)
                                                                                                                                                                                                from waterfall_pool
                                                                                                                                                                                                                                                
                         set rowcount @precision

                                                                                                                                                                                                                                                truncate table temp_lookup

                                                                                                                                                                                                                                                                insert into temp_lookup(account_number)
                                                                                                                                                                                                                                                                select li4.account_number
                                                                                                                                                                                                                                                                                from waterfall_pool as li4
                                                                                                                                                                                                                                                                                                                        inner join panbal_sav as sav on li4.account_number = sav.account_number
                                                                                                                                                                                                                                                order by case when sav.rq is null then case when cbck_rate is null then 1 else cbck_rate end when sav.rq > 1 then 1 else sav.rq end * ta_propensity desc

                         set rowcount 0

                      delete from waterfall_pool where account_number in (select account_number from temp_lookup)

                      insert into panbal_amends(account_number, movement)
                      select account_number
                            ,'Account to add to secondary panels for TA coverage'
                        from temp_lookup

                      select @ta = sum(ta_propensity)
                            ,@virtuals = count(1)
                        from panbal_sav as sav
                             left join panbal_amends  as pan on sav.account_number = pan.account_number
                       where panel is not null
                          or pan.account_number is not null

                      select @ta = @ta / @virtuals

               end --while

                                                                                                                        if (@records_out = 0) begin
                                                                                                                                                                        insert into panbal_amends(account_number
                                                                                                                                                                                                                                                                                                                                                                                ,movement)
                                                                                                                                                                        select 0
                           ,'Not enough accounts available to fill the turnaround propensity requirement'
                                                                                                                end

                                                                                                                -- check TA coverage - we also need at least 12% from accounts returning data on all panels
            select @ta = sum(ta_propensity)
              from panbal_sav as sav
                   left join panbal_amends  as pan on sav.account_number = pan.account_number
             where rq >= 0.5
               and (panel is not null or pan.account_number is not null)

            select @ta = @ta / sum(ta_propensity) from vespa_analysts.SkyBase_TA_scores

             while (@ta < @min_ta_ret and @records_out > 0) begin

                                                                                                                                                                                select @records_out = count(1)
                                                                                                                                                                                                from waterfall_pool
                                                                                                                                                                                                
                                                                                                                set rowcount @precision

                                                                                                                                                                                                                                                truncate table temp_lookup

                                                                                                                                                                                                                                                                insert into temp_lookup(account_number)
                                                                                                                                                                                                                                                                select li4.account_number
                                                                                                                                                                                                                                                                                from waterfall_pool as li4
                                                                                                                                                                                                                                                                                                                        inner join panbal_sav as sav on li4.account_number = sav.account_number
                                                                                                                                                                                                                                                order by case when sav.rq is null then case when cbck_rate is null then 1 else cbck_rate end when sav.rq > 1 then 1 else sav.rq end * ta_propensity desc

                         set rowcount 0

                      delete from waterfall_pool where account_number in (select account_number from temp_lookup)

                      insert into panbal_amends(account_number, movement)
                      select account_number
                            ,'Account to add to secondary panels for TA coverage'
                        from temp_lookup

                      select @ta = sum(ta_propensity)
                            ,@virtuals = count(1)
                        from panbal_sav as sav
                             left join panbal_amends  as pan on sav.account_number = pan.account_number
                       where panel is not null
                          or pan.account_number is not null

                      select @ta = @ta / @virtuals

               end --while

                                                                                                                        if (@records_out = 0) begin
                                                                                                                                                                        insert into panbal_amends(account_number
                                                                                                                                                                                                                                                                                                                                                                                ,movement)
                                                                                                                                                                        select 0
                           ,'Not enough accounts available to fill the turnaround propensity requirement'
                                                                                                                end
                                                                                                                        
                -- recreate list4
          truncate table waterfall_pool

            insert into waterfall_pool(
                   account_number
                  ,segment_id
                  ,rq
                  ,thi
            )
            select bas.account_number
                  ,segment_id
                  ,1
                  ,0
              from panbal_sav as bas
             where panel is null
          group by bas.account_number
                  ,segment_id

                -- New accounts to add to alternate day panels
            insert into panbal_amends(account_number, movement)
            select bas.account_number
                   ,'Account to add to secondary panels, eventually for primary panels'
              from PanBal_panel as bas
                   inner join waterfall_pool as li4 on bas.account_number = li4.account_number

                                                                                                                                -- drop unneeded tables
              drop table temp_lookup
              drop table temp_list1_rq                                                                                                          
              drop table temp_panels
              drop table temp_reqd
              drop table temp_available

     end; --V352_M07_VolCheck
        commit;

        grant execute on V352_M07_VolCheck to vespa_group_low_security;
        commit;
