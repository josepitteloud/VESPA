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
**Analysts:                             Jon Green   (Jonathan.Green@skyiq.co.uk)
                                        Leonardo Ripoli
**Lead(s):                              Hoi Yu Tang (hoiyu.tang@skyiq.co.uk)
**Stakeholder:                          Jose Loureda
**Project Code (Insight Collation):     V306
**SharePoint Folder:                    http://sp-department.bskyb.com/sites/IQSKY/SIG/Insight%20Collation%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FIQSKY%2FSIG%2FInsight%20Collation%20Documents%2F01%20Analysis%20Requests%2FV306%20-%20Foundation%20-%20Platform%20Maintenance%2FPhase%202%2FPanel%20Balancing

**Business Brief:

Panel Balancing is a regular exercise to ensure that the viewing panel is as
representative of the wider Sky customer base as possible as the latter evolves over
time. Balancing is also crucial in ensuring that key KPI and contractual obligations
of account coverage are maintained.




**Module:                              M01_Process_Manager

This module runs each of the modules required for balancing


*/
  create or replace procedure V352_M01_PanBal
         @max_imb          int   = 20
        ,@min_boxes        int   = 0        --to return per day
        ,@max_boxes        int   = 10000000 --to return per day
        ,@min_vp1b         int   = 0        --to return per day
        ,@min_ta_prop      float = 0
        ,@min_ta_returning float = 0
        ,@now              date  = today()
        ,@gen_schema       bit   = 0
        ,@prec             int   = 500
        ,@run_type         bit   = 0        --default 0 for daily metrics, 1 for full balancing
        ,@country_manager  bit   = 0        --default 0 for UK, 1 for ROI
      as begin

           declare @halt bit default 0

            select @halt = case when task like '%run finished' then 0 else 1 end
              from panbal_run_log
             where run_time = (
                      select max(run_time)
                        from panbal_run_log
                              )
                if @halt = 0 begin

                         set rowcount 0
                     execute(     'V352_M00_Initialise')
                      insert into panbal_run_log(run_time
                                                ,task
                                                ,notes
                                                 )
                      select now()
                            ,case when @country_manager = 1 then 'ROI ' end || case when @run_type = 1 then 'Full balancing run started'
                                                                                    else 'Daily metrics run started'
                                                                                end
                            ,''
                      commit

                     execute('call V352_M02_Waterfall           (@general_schema = @gen_schema
                                                                ,@today          = @now
                                                                 )')
                      insert into panbal_run_log(run_time
                                                ,task
                                                ,notes
                                                 )
                      select now()
                            ,case when @country_manager = 1 then 'ROI ' end || 'Waterfall module complete'
                            ,''
                      commit

                     execute(     'V352_M03_PanBal_Segments')
                      insert into panbal_run_log(run_time
                                                ,task
                                                ,notes
                                                 )
                      select now()
                            ,case when @country_manager = 1 then 'ROI ' end || 'Segments module complete'
                            ,''
                      commit

                     execute(     'V352_M05_VirtPan')
                      insert into panbal_run_log(run_time
                                                ,task
                                                ,notes
                                                 )
                      select now()
                            ,case when @country_manager = 1 then 'ROI ' end || 'VirtPan module complete'
                            ,''
                      commit

                     execute('call V352_M04_PanBal_SAV          (@r_type         = @run_type
                                                                ,@today          = @now
                                                                ,@country        = @country_manager
                                                                 )')
                      insert into panbal_run_log(run_time
                                                ,task
                                                ,notes
                                                 )
                      select now()
                            ,case when @country_manager = 1 then 'ROI ' end || 'SAV module complete'
                            ,''
                      commit

                          if @run_type = 1 begin
                               execute('call V352_M06_Main      (@max_imbalance  = @max_imb
                                                                ,@min_b          = @min_boxes
                                                                ,@precision      = @prec
                                                                ,@country        = @country_manager
                                                                 )')
                                insert into panbal_run_log(run_time
                                                          ,task
                                                          ,notes
                                                           )
                                select now()
                                      ,case when @country_manager = 1 then 'ROI ' end || 'Main module complete'
                                      ,''
                                commit

                               execute('call V352_M07_VolCheck  (@max_b          = @max_boxes
                                                                ,@min_vp1        = @min_vp1b
                                                                ,@min_ta         = @min_ta_prop
                                                                ,@min_ta_ret     = @min_ta_returning
                                                                 )')
                                insert into panbal_run_log(run_time
                                                          ,task
                                                          ,notes
                                                           )
                                select now()
                                      ,case when @country_manager = 1 then 'ROI ' end || 'VolCheck module complete'
                                      ,''
                                commit

                         end
                     execute('call V352_M08_Metrics             (@r_type         = @run_type
                                                                ,@today          = @now
                                                                ,@country        = @country_manager
                                                                 )')
                      insert into panbal_run_log(run_time
                                                ,task
                                                ,notes
                                                 )
                      select now()
                            ,case when @country_manager = 1 then 'ROI ' end || case when @run_type = 1 then 'Full balancing run finished'
                                                                                    else 'Daily metrics run finished'
                                                                               end
                            ,''
                      commit
           end
          else begin
                      insert into panbal_run_log(run_time
                                                ,task
                                                ,notes
                                                 )
                      select now()
                            ,'Failed attempt to start'
                            ,''
                     message cast(now() as timestamp)||' | Failed attempt to start' to client
                 end
         end; -- procedure V352_M01_PanBal
commit;

 grant execute on V352_M01_PanBal to vespa_group_low_security;
commit;

select count() from accs
/*
      -- To run the whole process
 execute ('  call V352_M01_PanBal(@max_imb          = 24
                                 ,@min_boxes        = 210465
                                 ,@max_boxes        = 10000000
                                 ,@min_vp1b         = 300000
                                 ,@min_ta_prop      = .25
                                 ,@min_ta_returning = .12
                                 ,@prec             = 500
                                 ,@gen_schema       = 0
                                 ,@run_type         = 0        --default 0 for daily metrics, 1 for full balancing
                                 ,@country_manager  = 0        --0 for UK, 1 for ROI
                                 )')

      -- after an run-time error, delete tables from the proc that was running, and run the following line
  insert into panbal_run_log(run_time
                            ,task
                            ,notes
                             )

  select now()
        ,'Manually set to run finished'
        ,''

*/




/* for testing:

 execute('call V352_M02_Waterfall  (@general_schema = 0
                                   ,@today          = today()
                                    )')

 execute('call V352_M04_PanBal_SAV (@r_type         = 0
                                   ,@today          = today()
                                   ,@country        = 1 --ROI
                                    )')

 execute('call V352_M06_Main       (@max_imbalance = 32
                                   ,@min_b         = 0
                                   ,@precision     = 500
                                   ,@country        = 0
                                    )')

 execute('call V352_M07_VolCheck   (@max_b      = 10000000
                                   ,@min_vp1    = 0
                                   ,@min_ta     = 0
                                   ,@min_ta_ret = 0
                                     )')

 execute('call V352_M08_Metrics    (@r_type         = 1
                                   ,@today          = today()
                                   ,@country        = 1 --ROI
                                    )')

 execute ('call V352_M09_Update_Movements_Log  (@first_enablement = ''2016-09-08''
                                   )')
*/




select * from panbal_run_log
select * from panbal_results
select * from panbal_metrics
select movement,count() from panbal_amends group by movement


