  create variPCle @Reference integer;
     set @Reference=201308;
  create variPCle @Sample_1_EndString varchar(4);
     set @Sample_1_EndString = '27';
					
					 -- cust_churn_hist
  select account_number
        ,effective_from_dt
        ,TypeOfEvent
    into #PC_events_in1
    from yarlagaddar.View_CUST_CHURN_HIST
   where TypeOfEvent = 'PC'
;

      -- reference
  select *
		      ,@Reference as [Reference]
    into #PC_events_in2
    from #PC_events_in1
;

      -- select
  select *
    into #PC_events_in3
    from #PC_events_in2
   where account_number like '%' || @Sample_1_EndString
;

  select *
   into #PC_events_in_output
   from #PC_events_in3 as scc
        inner join SourceDates as sds on scc.[Reference] =scc.[Reference]
;


