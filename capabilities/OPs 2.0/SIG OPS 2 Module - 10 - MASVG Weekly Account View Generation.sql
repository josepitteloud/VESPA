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

	This module assambles the optput from the production modules to generate the
	SIG_Single_Account_View and manage the history for the same context (Accounts)...

**Modules:

	M10: MASVG Account View Generator
        M10.0 - Initialising environment
		M10.1 - Assembling SAV weekly snapshot
		M10.2 - Assembling SAV historical snapshot
		M10.3 - DB Maintenance
        M10.4 - QAing results
		M10.5 - Setting Access Privileges
		M10.6 - Returning Results

**Sections:

--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M10.0 - Initialising environment
-----------------------------------

create or replace procedure sig_masvg_m10_accountview_generator
as begin

	MESSAGE cast(now() as timestamp)||' | Beginig M10.0 - Initialising environment' TO CLIENT
    
    -- Local Variables
    declare @weekending_tag     varchar(8)
    declare @weeklyview_name    varchar(16)
    declare @weekending         date
    declare @sql_               varchar(6000)
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
								
    select  @weekending =   case    when datepart(weekday,now()) = 7 then dateformat( now(), 'YYYY-MM-DD')
									else dateformat((now() - datepart(weekday,now())), 'YYYY-MM-DD')
							end
    --  Composing the name of the table
    set @weeklyview_name = 'SIG_SAV_'||@weekending_tag

    if object_id('vespa_analysts.'||@weeklyview_name) is not null --select 1 else select 0
        begin
            set @sql_ = 'drop table '||@weeklyview_name
            execute (@sql_)
            commit
        end


    set @sql_ = 'create table '||@weeklyview_name||'(

                    Weekending                  date            not null
                    ,Account_number             varchar(20)     not null
                    ,viewing_consent_flag       varchar(1)      default null
                    ,Scaling_segment_ID         integer         default null
                    ,Non_Scaling_segment_ID     integer         default null
                    ,Adsmart_flag               bit             default 0
                    ,Num_boxes                  tinyint         default 0
                    ,Num_adsmartable_boxes      tinyint         default 0
                    ,Panel                      varchar(10)     default null
                    ,Skygo_subs                 bit             default 0
                    ,Anytime_plus_subs          bit             default 0
                    ,Reporting_performance      varchar(20)     default null
                    ,Panel_recency              date            default null
                    ,avg_reporting_quality      decimal(15,3)   default null
                    ,min_reporting_quality      float           default null
					,scaling_reporting_quality	decimal(15,3)   default null
                    ,status_vespa               varchar(20)     default null
                    ,Panel_activation_date      date            default null
                    ,cb_key_individual          bigint          default null
                    ,Cust_active_DTV            bit             default 0
                    ,UK_Standard_account        varchar(3)      default null
                    ,box_type_subs              varchar(20)     default null
                    ,HD_box_subs                bit             default 0
                    ,RTM                        varchar(30)     default null
                    ,Weight                     float    		default null
					,viq_weight					float    		default null
                    ,Weight_date                date            default null
                    ,prem_sports                tinyint         default null
                    ,prem_movies                tinyint         default null
                    ,cust_active_dt             date            default null
					,num_ac_returned_30d		tinyint			default null
					,num_ac_returned_7d    		tinyint			default null
					,ac_full_returned_30d		bit				default 0
					,ac_full_returned_7d    	bit				default 0
					
                );

                create hg index '||@weeklyview_name||'_hg1 on '||@weeklyview_name||'(account_number);
                create hg index '||@weeklyview_name||'_hg2 on '||@weeklyview_name||'(Scaling_segment_ID);
                create hg index '||@weeklyview_name||'_hg3 on '||@weeklyview_name||'(Non_Scaling_segment_ID);'

    execute (@sql_)

    commit
    MESSAGE cast(now() as timestamp)||' | @ M10.0: Table ' || @weeklyview_name || ' creation DONE' TO CLIENT
    
    
    drop view if exists SIG_SINGLE_ACCOUNT_VIEW
    commit
    
    set @sql_ = 'create view SIG_SINGLE_ACCOUNT_VIEW as
                    select * from '||@weeklyview_name

    execute (@sql_)
    
    grant select on SIG_SINGLE_ACCOUNT_VIEW to vespa_group_low_security
    
    commit
    MESSAGE cast(now() as timestamp)||' | @ M10.0: SIG_SINGLE_ACCOUNT_VIEW Refreshment (now pointing to' ||@weeklyview_name||') DONE' TO CLIENT
    
    MESSAGE cast(now() as timestamp)||' | @ M10.0: Initialisation DONE' TO CLIENT
    
-----------------------------------------
-- M10.1 - Assembling SAV weekly snapshot
-----------------------------------------

    set @sql_ =     'insert into '||@weeklyview_name||' ('||
                                                                'weekending'||
                                                                ',account_number'||
                                                                ',viewing_consent_flag'||
                                                                ',Anytime_plus_subs'||
																',Reporting_performance'||
                                                                ',Cust_active_DTV'||
                                                                ',UK_Standard_account'||
                                                                ',box_type_subs'||
                                                                ',HD_box_subs'||
                                                                ',RTM'||
                                                                ',prem_sports'||
                                                                ',prem_movies'||
                                                                ',cust_active_dt'||
                                                                ',Scaling_segment_ID'||
                                                                ',Non_Scaling_segment_ID'||
                                                                ',Weight'||
                                                                ',Weight_date'||
                                                                ',adsmart_flag'||
                                                                ',num_boxes'||
                                                                ',Num_adsmartable_boxes'||
                                                                ',panel'||
																',status_vespa'||
																',num_ac_returned_7d'||
																',num_ac_returned_30d'||
																',avg_reporting_quality'||
																',min_reporting_quality'||
																',scaling_reporting_quality'||
																',viq_weight'||
																',ac_full_returned_30d'||
																',ac_full_returned_7d '||															
                                                            ')' ||
                    ' select  '''||@weekending||
                            ''',m08.account_number
                            ,m08.viewing_consent_flag
                            ,m08.Anytime_plus_subs
							,m05.Reporting_performance
                            ,m08.Cust_active_DTV
                            ,m08.UK_Standard_account
                            ,m08.box_type_subs
                            ,m08.HD_box_subs
                            ,m08.RTM
                            ,m08.prem_sports
                            ,m08.prem_movies
                            ,m08.cust_active_dt
                            ,m06.Scaling_segment_ID
                            ,m06.Non_Scaling_segment_ID
                            ,m06.Weight
                            ,m06.Weight_date
                            ,coalesce(m07.ads_flag,0) as adsmart_flag
                            ,m07.num_boxes
                            ,m07.Num_adsmartable_boxes
                            ,m04.panel
							,m04.status_vespa
							,m05.nacr7d
							,m05.nacr30d
							,m05.rep_qual
							,m05.min_reporting_quality
							,m05.rqs
							,m06.viq_weight
							,coalesce(m05.acf30d,0)
							,coalesce(m05.acf7d,0)
                    from    m08_t1_account_base_stage0              as m08
                            left join m06_t1_panel_balance_stage0   as m06
                            on  m08.account_number = m06.account_number
                            left join   (
                                            select  account_number
                                                    ,max(adsmart_flag)              as ads_flag
                                                    ,count(distinct subscriber_id)  as num_boxes
                                                    ,sum(adsmart_flag)              as Num_adsmartable_boxes
                                            from    m07_t1_box_base_stage0
                                            group   by  account_number
                                        )   as m07
                            on  m08.account_number = m07.account_number
                            left join   (
                                            select  account_number
                                                    ,panel
													,max(status_vespa)	as status_vespa
                                            from    m04_t1_panel_sample_stage0
                                            where   panel is not null
											group	by	account_number
														,panel
                                        )   as m04
                            on  m08.account_number = m04.account_number
                            left join   (
                                            select  mod05.account_number
                                                    ,avg(mod05.reporting_quality) as rep_qual
                                                    ,min(mod05.reporting_quality) as min_reporting_quality
                                                    ,case   when datediff(day, max(mod04.Enablement_date), '''||@profiling_thursday||''') < 15	then ''Recently enabled''
                                                            when min(mod05.return_data_30d) = 1                                           		then ''Acceptable''
                                                            when min(mod05.num_logs_sent_30d) >= 27 or min(mod05.reporting_quality) >= 0.9  	then ''Acceptable''
                                                            when max(mod05.num_logs_sent_30d) = 0                                           	then ''Zero reporting''
																																				else ''Unreliable''
                                                    end     as Reporting_performance
													,min(mod05.ac_full_returned_7d)		as acf7d
													,min(mod05.ac_full_returned_30d)	as acf30d
													,min(mod05.num_ac_returned_30d)		as nacr30d
													,min(mod05.num_ac_returned_7d)		as nacr7d
													,min(mod05.reporting_quality_s)	as rqs
                                            from    m05_t1_panel_performance_stage0         as mod05
                                                    inner join m04_t1_panel_sample_stage0   as mod04
                                                    on  mod05.account_number = mod04.account_number
                                            where   mod04.panel is not null
                                            group   by  mod05.account_number
                                        )   as m05
                            on  m08.account_number = m05.account_number'
	
	
    execute (@sql_)

    commit
    MESSAGE cast(now() as timestamp)||' | @ M10.1: Weekly Accounts Snapshot DONE' TO CLIENT


---------------------------------------------
-- M10.2 - Assembling SAV historical snapshot
---------------------------------------------
	
	MESSAGE cast(now() as timestamp)||' | Beginig M10.2 - Assembling SAV historical snapshot' TO CLIENT
	
	
	if exists	(
					select  tname
					from    sys.syscatalog
					where   upper(tname) like 'SIG_SAV_%'
					and     cast(right(tname,8) as date) between cast((@weekending-105) as date) and cast((@weekending) as date)
					and		creator = 'vespa_analysts'
				)
	begin
	
		MESSAGE cast(now() as timestamp)||' | @ M10.2: Historical tables found' TO CLIENT
	
		-- dropping the view to refresh it
		drop view if exists SIG_SAV_HIST_VIEW
		set @sql_ = ''	
		commit
		
		-- querying the syscatalog to check what are the last 4 weeks available for the history
		declare thecursor cursor for
			
			-- Creating a list of tables fit for historical view (only 3 month)...
			select  tname
			from    sys.syscatalog
			where   upper(tname) like 'SIG_SAV_%'
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

		set @sql_ = 'create view SIG_SAV_HIST_VIEW as '|| left(@sql_,(length(@sql_)-10))

		-- refreshing the view
		execute (@sql_)
		set @hist_access = 1
		commit
		
		MESSAGE cast(now() as timestamp)||' | @ M10.2: Historical Snapshot for Accounts DONE' TO CLIENT
	end
	else
	begin
		MESSAGE cast(now() as timestamp)||' | @ M10.2: No Historical Tables available, Historical View NOT Created' TO CLIENT
	end
	
	commit

	MESSAGE cast(now() as timestamp)||' | @ M10.2 - Assembling SAV historical snapshot DONE' TO CLIENT
	
-------------------------
-- M10.3 - DB Maintenance
-------------------------

	-- NYIP!
	-- dropping old weekly snapshot that are out-with the scope (anything older than 3 month)

------------------------
-- M10.4 - QAing results
------------------------

------------------------------------
-- M10.5 - Setting Access Privileges
------------------------------------

    set @sql_ = 'grant select on '||@weeklyview_name||' to vespa_group_low_security'
    execute (@sql_)
    grant select on SIG_SINGLE_ACCOUNT_VIEW to vespa_group_low_security
	grant select on SIG_SAV_HIST_VIEW 		to vespa_group_low_security
    commit

----------------------------
-- M10.6 - Returning results
----------------------------

    MESSAGE cast(now() as timestamp)||' | M10 Finished' TO CLIENT

    commit
	
end;

commit;
grant execute on sig_masvg_m10_accountview_generator to vespa_group_low_security;
commit;