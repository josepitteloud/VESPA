select * into #stb_active from
     (select account_number
            ,x_model_number
            ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) rank
        from sk_prod.cust_Set_top_box) as sub
 where rank = 1
--24,491,854

select count(distinct bas.account_number)
  from panbal_amends as bas
       inner join #stb_active as stb on bas.account_number = stb.account_number
 where movement='Account to add to panels 6/7, eventually for panel 12'
   and x_model_number = 'DRX 595'
--17,927 of these 40k accounts will have to be removed

--with Tony's table
select count(distinct bas.account_number)
  from panbal_amends as bas
       inner join DRX_595_BOXES_LIST as stb on bas.account_number = stb.account_number
 where movement='Account to add to panels 6/7, eventually for panel 12'
--17,487

  select panel
        ,sbv.account_number
    into panel_removes
    from vespa_analysts.vespa_single_box_view as sbv
         inner join #stb_active as stb on sbv.account_number = stb.account_number
   where status_vespa='Enabled'
     and x_model_number = 'DRX 595'
group by panel
        ,sbv.account_number
--41,436 accounts on any panel

execute waterfall

  select count(distinct sbv.account_number)
    from vespa_analysts.vespa_single_box_view as sbv
         inner join #stb_active as stb on sbv.account_number = stb.account_number
         inner join vespa_analysts.waterfall_base as wat on wat.account_number = sbv.account_number
   where status_vespa='Enabled'
     and (x_model_number = 'DRX 595' or l22_known_prefix <> 1 or l23_empty_prefix <> 1)
--507,499
count(distinct sbv.account_number)
1711752

select top 10 * from vespa_analysts.waterfall_base
select knockout_level,l22_known_prefix,l23_empty_prefix,count(1) from vespa_analysts.waterfall_base group by knockout_level,l22_known_prefix,l23_empty_prefix


select top 10 * from sk_prod.VESPA_DP_PROG_VIEWED_CURRENT

