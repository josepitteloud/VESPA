/*
Vespa panellists – overall and split out into  Sports(dual sports & Top Tier )  and BB subscribers ( both with and without Sport)
Total panellists enabled (all panels) - overall and split out into  Sports  and BB subscribers

Total enabled panellists that have returned data (ever)
Total enabled panellists that have returned day for at a least 30% of days in the last 6 weeks
*/


-----------------
-- INITIALISATION
-----------------

      -- Initialise base table
  create table #vespa_sports_bb(
         account_number varchar(30)
        ,viewing_panel   bit default 0
        ,sports          bit default 0
        ,bb              bit default 0
        ,data_returned   bit default 0
        ,data_30pc       bit default 0
);

  create unique hg index uhacc on #vespa_sports_bb(account_number);


      -- Add accounts from SBV and identify whether they're on the Viewing panel or otherwise
  insert into #vespa_sports_bb(
         account_number
        ,viewing_panel
         )
  select account_number
        ,case when panel_id_vespa in (11, 12) then 1 else 0 end as viewing_panel
    from vespa_analysts.vespa_single_box_view
   where status_vespa = 'Enabled'
group by account_number
        ,viewing_panel
;



------------------------------------
-- GET SPORTS SUBSCRIBERS (ACCOUNTS)
------------------------------------

      -- Get all Vespa accounts with active sports subscriptions
  select bas.account_number
    into #sports
    from #vespa_sports_bb as bas
         inner join cust_subs_hist as csh on bas.account_number = csh.account_number
         inner join cust_entitlement_lookup as cel on csh.current_short_description = cel.short_description
   where csh.subscription_sub_type ='DTV Primary Viewing'
     and csh.status_code in ('AC','AB','PC')
     and csh.effective_to_dt = '9999-09-09'
     and prem_sports = 2
;


      -- Update the active Sports subscriber flag on the base table
  update #vespa_sports_bb as bas
     set sports = 1
    from #sports as spo
   where bas.account_number = spo.account_number
;


---------------------------------------
-- GET BROADBAND SUBSCRIBERS (ACCOUNTS)
---------------------------------------

      -- Now get the active BB package accounts
  select bas.account_number
    into #bb
    from cust_subs_hist as csh
         inner join #vespa_sports_bb as bas on bas.account_number = csh.account_number
   where subscription_sub_type = 'Broadband DSL Line'
     and csh.effective_to_dt   =  '9999-09-09'
     and (status_code in ('AC','AB') or (status_code='PC' and prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
          or (status_code='CF' and prev_status_code='PC')
          or (status_code='AP' and sale_type='SNS Bulk Migration')
         )
;

      -- Update the active Broadband subscriber flag on the base table
  update #vespa_sports_bb as bas
     set bb = 1
    from #bb as bba
   where bas.account_number = bba.account_number
;


--------------------------------
-- Data returns / Scaling weight
--------------------------------

      -- Update data return flag for any account that appears in the VIQ data scaling table
  update #vespa_sports_bb as bas
     set data_returned = 1
    from viq_viewing_data_scaling as viq
   where bas.account_number = viq.account_number
;


      -- Get accounts that have a scaling weight within the last 6 weeks
  select account_number
        ,count(1) as days
    into #returns
    from viq_viewing_data_scaling
   where adjusted_event_start_date_vespa >= today() - 42
group by account_number
  having days > 12 --30%
;


      -- Update recent data return flag in base table
  update #vespa_sports_bb as bas
     set data_30pc = 1
    from #returns as ret
   where bas.account_number = ret.account_number
;



--------------------
-- Summary for pivot
--------------------

  select count()
        ,case when viewing_panel = 1 then 'Yes' else 'No' end
        ,case when sports        = 1 then 'Yes' else 'No' end
        ,case when bb            = 1 then 'Yes' else 'No' end
        ,case when data_returned = 1 then 'Yes' else 'No' end
        ,case when data_30pc     = 1 then 'Yes' else 'No' end
    from #vespa_sports_bb
group by viewing_panel
        ,sports
        ,bb
        ,data_returned
        ,data_30pc
order by viewing_panel
        ,sports
        ,bb
        ,data_returned
        ,data_30pc
;










