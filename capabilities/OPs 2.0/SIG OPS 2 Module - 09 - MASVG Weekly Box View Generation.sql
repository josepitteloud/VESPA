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
**Project Name:							OPS 2.0
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jose Loureda
**Stakeholder:                          Operational Reports / SIG
**Due Date:                             20/09/2013
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:

**Modules:

	M09: MASVG Box View Generator
        M09.0 - Initialising environment
		M09.1 - Assembling SBV weekly snapshot
		M09.2 - Assembling SBV historical snapshot
        M09.2 - QAing results
		M09.3 - Setting Access Privileges
		M09.4 - Returning Results
		
**Sections:

--------------------------------------------------------------------------------------------------------------
*/


-----------------------------------
-- M09.0 - Initialising environment
-----------------------------------

create or replace procedure sig_masvg_m09_boxview_generator
as begin

    MESSAGE cast(now() as timestamp)||' | Beginig M09.0 - Initialising environment' TO CLIENT
    
    -- Local Variables
    declare @weekending_tag     varchar(8)
    declare @weeklyview_name    varchar(16)
    declare @weekending         date
    declare @sql_               varchar(5000)
    declare @profiling_thursday date
    declare @tname 				varchar(20)
	declare @hist_access		bit

    -- A Saturday...
    execute vespa_analysts.Regulars_Get_report_end_date @profiling_thursday output 
    -- so we make it the Thursday before...
    set @profiling_thursday    = dateadd(day, -8, @profiling_thursday)

    select  @weekending_tag =   case    when datepart(weekday,now()) = 7
                                        then dateformat( now(), 'YYYYMMDD')
                                        else dateformat((now() - datepart(weekday,now())), 'YYYYMMDD')
                                end
    select  @weekending =   case    when datepart(weekday,now()) = 7
                                    then dateformat( now(), 'YYYY-MM-DD')
                                    else dateformat((now() - datepart(weekday,now())), 'YYYY-MM-DD')
                            end
    --  Composing the name of the table
    set @weeklyview_name = 'SIG_SBV_'||@weekending_tag

   if object_id('vespa_analysts.'||@weeklyview_name) is not null --select 1 else select 0
        begin
            set @sql_ = 'drop table '||@weeklyview_name
            execute (@sql_)
            commit
        end
        
    set @sql_ = 'create table '||@weeklyview_name||'(

                    Weekending                      date            not null
                    ,Subscriber_id                  decimal(10)     default null
                    ,Card_Subscriber_ID             varchar(10)     default null
                    ,Account_number                 varchar(20)     not null
                    ,Service_instance_ID            varchar(30)     default null
                    ,consumerview_cb_row_id         bigint          default null
                    ,Panel                          varchar(10)     default null
                    ,Panel_ID_4_cells_confirm       bit             default 0
                    ,Is_Sky_view_candidate          bit             default 0
                    ,Is_Sky_view_selected           bit             default 0
                    ,status_vespa                   varchar(20)     default null
                    ,Enablement_date                date            default null
                    ,Enablement_date_source         varchar(20)     default null
                    ,vss_request_dt                 date            default null
                    ,Sky_view_load_date             date            default null
                    ,historic_result_date           date            default null
                    ,Selection_date                 date            default null
                    ,vss_created_date               date            default null
                    ,Num_logs_sent_30d              integer         default null
                    ,Num_logs_sent_7d               integer         default null
                    ,Continued_trans_30d            integer         default null
                    ,Continued_trans_7d             integer         default null
					,returned_data_30d				tinyint			default null
					,returned_data_7d    			tinyint			default null
                    ,reporting_quality              decimal(15,3)   default null
                    ,PS_Olive                       varchar(1)      default null
                    ,PS_vespa                       varchar(1)      default null
                    ,PS_inferred_primary            bit             default 0
                    ,ps_flag                        varchar(1)      default null
                    ,ps_source                      varchar(10)     default null
                    ,box_type_physical              varchar(20)     default null
                    ,HD_box_physical                bit             default 0
                    ,box_storage_capacity           varchar(20)     default null
                    ,Box_is_3D                      bit             default 0
                    ,Box_has_anytime_plus           bit             default 0
                    ,Scaling_segment_ID             integer         default null    
                    ,Non_Scaling_segment_ID         integer         default null
                    ,Box_model                      varchar(20)     default null
                    ,Adsmart_flag                   bit             default 0
					,description					varchar(50)		default null

                );

                create date index '||@weeklyview_name||'_dt1 on '||@weeklyview_name||'(weekending);
                create hg index '||@weeklyview_name||'_hg1 on '||@weeklyview_name||'(subscriber_id);
                create hg index '||@weeklyview_name||'_hg2 on '||@weeklyview_name||'(card_subscriber_id);
                create hg index '||@weeklyview_name||'_hg3 on '||@weeklyview_name||'(account_number);
                create hg index '||@weeklyview_name||'_hg4 on '||@weeklyview_name||'(Scaling_segment_ID);
                create hg index '||@weeklyview_name||'_hg5 on '||@weeklyview_name||'(Non_Scaling_segment_ID);'
            
    --select @sql_  
    execute (@sql_)

    commit
    MESSAGE cast(now() as timestamp)||' | @ M09.0: Table ' || @weeklyview_name || ' creation DONE' TO CLIENT

    drop view if exists SIG_SINGLE_BOX_VIEW
    commit
    
    set @sql_ = 'create view SIG_SINGLE_BOX_VIEW as
                    select * from '||@weeklyview_name

    execute (@sql_)
    
    grant select on SIG_SINGLE_BOX_VIEW to vespa_group_low_security
    
    commit
    MESSAGE cast(now() as timestamp)||' | @ M09.0: SIG_SINGLE_BOX_VIEW Refreshment (now pointing to' ||@weeklyview_name||') DONE' TO CLIENT
    
    MESSAGE cast(now() as timestamp)||' | @ M09.0: Initialisation DONE' TO CLIENT

-----------------------------------------
-- M09.1 - Assembling SBV weekly snapshot
-----------------------------------------

	MESSAGE cast(now() as timestamp)||' | Beginig M09.1 - Assembling SBV weekly snapshot' TO CLIENT

    set @sql_ = 'insert  into '||@weeklyview_name||'('||
                                                        'weekending'||
                                                        ',Subscriber_id'||
                                                        ',Card_Subscriber_ID'||
                                                        ',Account_number'||
                                                        ',Service_instance_ID'||
                                                        ',box_type_physical'||
                                                        ',HD_box_physical'||
                                                        ',box_storage_capacity'||
                                                        ',Box_is_3D'||
                                                        ',Box_has_anytime_plus'||
                                                        ',Box_model'||
                                                        ',Adsmart_flag'||
                                                        ',panel'||
                                                        ',Panel_ID_4_cells_confirm'||
                                                        ',Is_Sky_view_candidate'||
                                                        ',Is_Sky_view_selected'||
                                                        ',status_vespa'||
                                                        ',Enablement_date'||
                                                        ',Enablement_date_source'||
                                                        ',vss_request_dt'||
                                                        ',Sky_view_load_date'||
                                                        ',historic_result_date'||
                                                        ',Selection_date'||
                                                        ',vss_created_date'||
                                                        ',PS_Olive'||
                                                        ',PS_vespa'||
                                                        ',PS_inferred_primary'||
                                                        ',ps_flag'||
                                                        ',ps_source'||
                                                        ',Num_logs_sent_30d'||
                                                        ',Num_logs_sent_7d'||
                                                        ',Continued_trans_30d'||
                                                        ',Continued_trans_7d'||
                                                        ',reporting_quality'||
                                                        ',Scaling_segment_ID'||
                                                        ',Non_Scaling_segment_ID'||
														',description'||
														',returned_data_30d'||
														',returned_data_7d '||														
                                                    ')'||
                ' select  '''||@weekending||
                        ''',m07.Subscriber_id
                        ,m07.Card_Subscriber_ID
                        ,m07.Account_number
                        ,m07.Service_instance_ID
                        ,m07.box_type_physical
                        ,coalesce(m07.HD_box_physical,0)
                        ,m07.box_storage_capacity
                        ,coalesce(m07.Box_is_3D,0)
                        ,coalesce(m07.Box_has_anytime_plus,0)
                        ,m07.Box_model
                        ,coalesce(m07.Adsmart_flag,0)
                        ,m04.panel
                        ,coalesce(m04.Panel_ID_4_cells_confirm,0)
                        ,coalesce(m04.Is_Sky_view_candidate,0)
                        ,coalesce(m04.Is_Sky_view_selected,0)
                        ,m04.status_vespa           
                        ,m04.Enablement_date        
                        ,m04.Enablement_date_source 
                        ,m04.vss_request_dt         
                        ,m04.Sky_view_load_date     
                        ,m04.historic_result_date   
                        ,m04.Selection_date         
                        ,m04.vss_created_date
                        ,m04.PS_Olive
                        ,m04.PS_vespa
                        ,coalesce(m04.PS_inferred_primary,0) as PS_inferred_primary
                        ,m04.ps_flag
                        ,m04.ps_source
                        ,m05.Num_logs_sent_30d
                        ,m05.Num_logs_sent_7d
                        ,m05.Continued_trans_30d
                        ,m05.Continued_trans_7d
                        ,m05.reporting_quality
                        ,m06.Scaling_segment_ID     
                        ,m06.Non_Scaling_segment_ID
						,m07.description
						,m05.return_data_30d
						,m05.return_data_7d
                from    m07_t1_box_base_stage0                      as m07
                        left join m04_t1_panel_sample_stage0        as m04
                        on  m07.account_number      = m04.account_number
                        and m07.card_subscriber_id  = m04.card_subscriber_id
                        left join m05_t1_panel_performance_stage0   as m05
                        on  m07.account_number      = m05.account_number
                        and m07.subscriber_id  = m05.subscriber_id
                        left join m06_t1_panel_balance_stage0       as m06
                        on  m07.account_number      = m06.account_number'

    execute (@sql_)

    commit
               
    MESSAGE cast(now() as timestamp)||' | @ M09.1: Weekly Snapshot for Boxes DONE' TO CLIENT

---------------------------------------------
-- M09.2 - Assembling SBV historical snapshot
---------------------------------------------
	
	MESSAGE cast(now() as timestamp)||' | Beginig M09.2 - Assembling SBV historical snapshot' TO CLIENT
	
	
	if exists	(
					select  tname
					from    sys.syscatalog
					where   upper(tname) like 'SIG_SBV_%'
					and     cast(right(tname,8) as date) between cast((@weekending-105) as date) and cast((@weekending) as date)
					and		creator = 'vespa_analysts'
				)
	begin
			
		MESSAGE cast(now() as timestamp)||' | @ M09.2: Historical tables found' TO CLIENT
		
		-- dropping the view to refresh it
		drop view if exists SIG_SBV_HIST_VIEW 
		set @sql_ = ''
		commit
		
		-- querying the syscatalog to check what are the last 4 weeks available for the history
		declare thecursor cursor for        
        
			-- Creating a list of tables fit for historical view (only 3 month)...
			select  tname
			from    sys.syscatalog
			where   upper(tname) like 'SIG_SBV_%'
			and     cast(right(tname,8) as date) between cast((@weekending-105) as date) and cast((@weekending) as date)
			and		creator = 'vespa_analysts'
			order   by  tname
			
		for read only

		open thecursor
		fetch next thecursor into @tname
        
		while (sqlstate = 0)
		begin

			set @sql_ = @sql_ ||'select * from vespa_analysts.'||@tname|| ' union all '

			fetch next thecursor into @tname

		end

		deallocate thecursor
		
		set @sql_ = 'create view SIG_SBV_HIST_VIEW as '|| left(@sql_,(length(@sql_)-10))
        
		-- refreshing the view
		execute (@sql_)
		set @hist_access = 1
		commit
		
		MESSAGE cast(now() as timestamp)||' | @ M09.2: Historical Snapshot for Boxes DONE' TO CLIENT
	end
	else
	begin
		MESSAGE cast(now() as timestamp)||' | @ M09.2: No Historical Tables available, Historical View NOT Created' TO CLIENT
	end
	
	commit

	MESSAGE cast(now() as timestamp)||' | @ M09.2 - Assembling SBV historical snapshot DONE' TO CLIENT
	
------------------------
-- M09.3 - QAing results
------------------------

------------------------------------
-- M09.4 - Setting Access Privileges
------------------------------------

	MESSAGE cast(now() as timestamp)||' | Beginig M09.4 - Setting Access Privileges' TO CLIENT

    set @sql_ = 'grant select on '||@weeklyview_name||' to vespa_group_low_security'
    execute (@sql_)
    grant select on SIG_SINGLE_BOX_VIEW to vespa_group_low_security
	
	if @hist_access = 1
		grant select on SIG_SBV_HIST_VIEW	to vespa_group_low_security
    
	commit

	MESSAGE cast(now() as timestamp)||' | @ M09.4 - Setting Access Privileges DONE' TO CLIENT
	
----------------------------
-- M09.5 - Returning Results
----------------------------

    MESSAGE cast(now() as timestamp)||' | M09 Finished' TO CLIENT

    commit

end;

commit;
grant execute on sig_masvg_m09_boxview_generator to vespa_group_low_security;
commit;