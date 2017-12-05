  -- ########################################################################
  -- #### Capping State 1 - create AUG tables - CUSTOM                           ####
  -- ########################################################################
		--Check that there is data in viwing table
select max(effective_from_dt) from sk_prod.cust_subs_hist;
select cast(dt_min / 100 as varchar(10)) as dt_min, cast(dt_max / 100 as varchar(10)) as dt_max
  from (select min(dk_event_start_datehour_dim) as dt_min,
               max(dk_event_start_datehour_dim) as dt_max
          from sk_prod.vespa_dp_prog_viewed_current) a;


  -- ########################################################################
  -- #### Capping State 2 - create AUG tables - CUSTOM                           ####
  -- ########################################################################
		-- Change months according to range of run
drop view if exists Capping2_00_Raw_Uncapped_Events;
create view Capping2_00_Raw_Uncapped_Events as
  select *
    from sk_prod.VESPA_DP_PROG_VIEWED_201403
   where panel_id = 12
     and type_of_viewing_event <> 'Non viewing event'
     and type_of_viewing_event is not null
  union all
  select *
    from sk_prod.VESPA_DP_PROG_VIEWED_201404
   where panel_id = 12
     and type_of_viewing_event <> 'Non viewing event'
     and type_of_viewing_event is not null;
commit;

	-- Change dates according to range being run
begin

    declare @varBuildId int
    declare @varStartDate date
    declare @varEndDate   date

    set @varStartDate = '2014-03-30'      -- A Friday
    set @varEndDate   = '2014-04-30'      -- The following Thursday

    execute logger_create_run 'Capping2.x CUSTOM', 'Weekly capping run', @varBuildId output
    commit

    execute CP2_Profile_Boxes_CUSTOM @varStartDate, @varBuildId
    commit

    while @varStartDate <= @varEndDate
        begin
            execute CP2_build_days_caps_CUSTOM @varStartDate, @varBuildId
            --execute vespa_analysts.cap_getScalingByDay_v01 @varStartDate, 'Vespa_Daily_Augs_', 'Scaling2->AUG', @varBuildId
            commit

            set @varStartDate = @varStartDate + 1
        end

    execute CP2_clear_transient_tables

    execute logger_get_latest_job_events 'Capping2.x CUSTOM', 4

end;








