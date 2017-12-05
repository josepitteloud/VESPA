      -- update panel counts with the proposed panel
  select segment_id
        ,sav.account_number
        ,sum(case when sav.rq is null then case when cbck_rate is null then 1
                                                                       else cbck_rate
                                           end
                                      when sav.rq > 1 then 1
                                      else sav.rq
                                 end) as cow
    into #current_accs
    from vespa_analysts.panbal_sav as sav
         inner join panbal_weekly_sample              as sam on sav.account_number = sam.account_number
         left join vespa_analysts.panel_movements_log as log on sav.account_number = log.account_number
   where panel in (11,12)
     and (requested_movement_type not in ('Panel Balancing', 'Panel Balancing alt.')
          or requested_movement_type is null)
group by segment_id
        ,sav.account_number
;

  select segment_id
        ,sum(cow) as cow
    into #current
    from #current_accs
group by segment_id
;

  update PanBal_segments_lookup as bas
     set panel_accounts = 0
;

  update PanBal_segments_lookup as bas
     set panel_accounts = cur.cow
    from #current as cur
   where bas.segment_id = cur.segment_id
;

  create table #psl(segment_id int
        ,adsmbl            varchar(30)   default 'Non-Adsmartable'
        ,region            varchar(40)
        ,hhcomp            varchar(30)   default 'U'
        ,tenure            varchar(30)
        ,package           varchar(30)
        ,mr                bit           default 0
        ,hd                bit           default 0
        ,pvr               bit           default 0
        ,valseg            varchar(30)   default 'Unknown'
        ,mosaic            varchar(30)   default 'U'
        ,fss               varchar(30)   default 'U'
        ,onnet             bit           default 0
        ,skygo             bit           default 0
        ,st                bit           default 0
        ,bb                bit           default 0
        ,bb_capable        varchar(8)
        ,panel_accounts    decimal(10,2) default 0
        ,base_accounts     int           default 0
);

insert into #psl(segment_id
        ,adsmbl
        ,region
        ,hhcomp
        ,tenure
        ,package
        ,mr
        ,hd
        ,pvr
        ,valseg
        ,mosaic
        ,fss
        ,onnet
        ,skygo
        ,st
        ,bb
        ,bb_capable
        ,base_accounts
        ,panel_accounts
        )
  select segment_id
        ,adsmbl
        ,region
        ,hhcomp
        ,tenure
        ,package
        ,mr
        ,hd
        ,pvr
        ,valseg
        ,mosaic
        ,fss
        ,onnet
        ,skygo
        ,st
        ,bb
        ,bb_capable
        ,sum(base_accounts)
        ,sum(panel_accounts)
    from panbal_segments_lookup as lkp
group by segment_id
        ,adsmbl
        ,region
        ,hhcomp
        ,tenure
        ,package
        ,mr
        ,hd
        ,pvr
        ,valseg
        ,mosaic
        ,fss
        ,onnet
        ,skygo
        ,st
        ,bb
        ,bb_capable
;

  drop table panbal_qa_all_aggregated_results;
  drop table panbal_qa_traffic_lights;
create table panbal_qa_all_aggregated_results(aggregation_variable varchar(30)
                                             ,variable_value       varchar(60)
                                             ,panel_reporters      decimal(10,2)
                                             ,sky_base             int
                                             ,ghi                  double default 0
);

  insert into panbal_qa_all_aggregated_results(aggregation_variable,variable_value,panel_reporters,sky_base)
  select 'adsmbl'
        ,adsmbl
        ,sum(panel_accounts)
        ,sum(base_Accounts)
    from #psl
group by adsmbl
;

  insert into panbal_qa_all_aggregated_results(aggregation_variable,variable_value,panel_reporters,sky_base)
  select 'region'
        ,region
        ,sum(panel_accounts)
        ,sum(base_Accounts)
    from #psl
group by region
;

  insert into panbal_qa_all_aggregated_results(aggregation_variable,variable_value,panel_reporters,sky_base)
  select 'hhcomp'
        ,hhcomp
        ,sum(panel_accounts)
        ,sum(base_Accounts)
    from #psl
group by hhcomp
;

  insert into panbal_qa_all_aggregated_results(aggregation_variable,variable_value,panel_reporters,sky_base)
  select 'tenure'
        ,tenure
        ,sum(panel_accounts)
        ,sum(base_Accounts)
    from #psl
group by tenure
;

  insert into panbal_qa_all_aggregated_results(aggregation_variable,variable_value,panel_reporters,sky_base)
  select 'package'
        ,package
        ,sum(panel_accounts)
        ,sum(base_Accounts)
    from #psl
group by package
;

  insert into panbal_qa_all_aggregated_results(aggregation_variable,variable_value,panel_reporters,sky_base)
  select 'mr'
        ,mr
        ,sum(panel_accounts)
        ,sum(base_Accounts)
    from #psl
group by mr
;

  insert into panbal_qa_all_aggregated_results(aggregation_variable,variable_value,panel_reporters,sky_base)
  select 'hd'
        ,hd
        ,sum(panel_accounts)
        ,sum(base_Accounts)
    from #psl
group by hd
;

  insert into panbal_qa_all_aggregated_results(aggregation_variable,variable_value,panel_reporters,sky_base)
  select 'pvr'
        ,pvr
        ,sum(panel_accounts)
        ,sum(base_Accounts)
    from #psl
group by pvr
;

  insert into panbal_qa_all_aggregated_results(aggregation_variable,variable_value,panel_reporters,sky_base)
  select 'valseg'
        ,valseg
        ,sum(panel_accounts)
        ,sum(base_Accounts)
    from #psl
group by valseg
;

  insert into panbal_qa_all_aggregated_results(aggregation_variable,variable_value,panel_reporters,sky_base)
  select 'mosaic'
        ,mosaic
        ,sum(panel_accounts)
        ,sum(base_Accounts)
    from #psl
group by mosaic
;

  insert into panbal_qa_all_aggregated_results(aggregation_variable,variable_value,panel_reporters,sky_base)
  select 'fss'
        ,fss
        ,sum(panel_accounts)
        ,sum(base_Accounts)
    from #psl
group by fss
;

  insert into panbal_qa_all_aggregated_results(aggregation_variable,variable_value,panel_reporters,sky_base)
  select 'onnet'
        ,onnet
        ,sum(panel_accounts)
        ,sum(base_Accounts)
    from #psl
group by onnet
;

  insert into panbal_qa_all_aggregated_results(aggregation_variable,variable_value,panel_reporters,sky_base)
  select 'skygo'
        ,skygo
        ,sum(panel_accounts)
        ,sum(base_Accounts)
    from #psl
group by skygo
;

  insert into panbal_qa_all_aggregated_results(aggregation_variable,variable_value,panel_reporters,sky_base)
  select 'st'
        ,st
        ,sum(panel_accounts)
        ,sum(base_Accounts)
    from #psl
group by st
;

  insert into panbal_qa_all_aggregated_results(aggregation_variable,variable_value,panel_reporters,sky_base)
  select 'bb'
        ,bb
        ,sum(panel_accounts)
        ,sum(base_Accounts)
    from #psl
group by bb
;

  insert into panbal_qa_all_aggregated_results(aggregation_variable,variable_value,panel_reporters,sky_base)
  select 'bb_capable'
        ,bb_capable
        ,sum(panel_accounts)
        ,sum(base_Accounts)
    from #psl
group by bb_capable
;


  create variable @panel_reporters int;
  create variable @total_sky_base int;

  select @panel_reporters = sum(panel_reporters)
    from panbal_qa_all_aggregated_results
    where aggregation_variable = 'adsmbl'
;

  select @total_sky_base = sum(sky_base)
    from panbal_qa_all_aggregated_results
   where aggregation_variable = 'fss'
;

  delete from panbal_qa_all_aggregated_results
   where variable_value in ('Non-scalable', 'NS', 'U', 'Not Defined', 'D) Unknown', 'Unknown')
      or variable_value is null
      or sky_base < 1000
      or sky_base is null
;

  create table #panel_households(
         aggregation_variable                               varchar(30)
        ,Panel_Households                                   decimal(10,2)
        ,sky_base_households                                int
);

  insert into #panel_households(aggregation_variable
                               ,Panel_Households
                               ,sky_base_households
         )
  select aggregation_variable
        ,sum(panel_reporters)
        ,sum(sky_base)
    from panbal_qa_all_aggregated_results
group by aggregation_variable
;

  update panbal_qa_all_aggregated_results as bas
     set ghi = 100 * panel_reporters * hsh.sky_base_households / (1.0 * Sky_Base * hsh.panel_households)
    from #panel_households as hsh
   where bas.aggregation_variable = hsh.aggregation_variable
;

  create table panbal_qa_traffic_lights(
         variable_name                                      varchar(30)
        ,imbalance_rating                                   decimal(10,2)
);

  insert into panbal_qa_traffic_lights(
              variable_name
             ,imbalance_rating
             )
            select aggregation_variable
                  ,sqrt(avg((ghi - 100) * (ghi - 100)))
              from Panbal_qa_all_aggregated_results
          group by aggregation_variable
;

select * from panbal_qa_traffic_lights
order by variable_name;

select * from panbal_qa_all_aggregated_results;
---




select count(1) from #current_accs

