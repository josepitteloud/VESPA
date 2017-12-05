       select ccs.account_number
             ,ccs.card_subscriber_id
         into #sky_box_count
         from sk_prod.CUST_CARD_SUBSCRIBER_LINK as ccs
              inner join sk_prod.cust_single_account_view as sav on ccs.account_number = sav.account_number
              inner join sk_prod.vespa_subscriber_status as vss on ccs.account_number = vss.account_number --anyone who has ever been on the panels
        where effective_to_dt = '9999-09-09'
          and cust_active_dtv = 1
     group by ccs.account_number
             ,ccs.card_subscriber_id
;--2,318,983

create hg index idx1 on #sky_box_count(card_subscriber_id);

select account_number
      ,card_subscriber_id
      ,panel_no
  into #vss
  from sk_prod.vespa_subscriber_status
 where result='Enabled'
;

  select box.account_number
        ,box.card_subscriber_id
        ,panel_no
    into #results
    from #sky_box_count as box
         left join #vss on box.card_subscriber_id = #vss.card_subscriber_id
;--2,318,983

  select account_number
        ,count(1)                                              as cow  --the number of boxes expected      per account
        ,sum(case when panel_no in (6,7,12) then 1 else 0 end) as vesp --the number of boxes on the panels per account
        ,max(panel_no) as panel
    into #r2
    from #results
group by account_number
;--1,751,520

select sum(cow),sum(vesp) from #r2
where vesp>0;
select 2086168 - 2045550
;--40,618 boxes missing from the panels

  select account_number
        ,panel
    into missing_boxes_table1
    from #r2
   where vesp>0
     and cow>vesp
group by account_number,panel
;--37,315 accounts

  select bas.account_number
        ,bas.panel
        ,card_subscriber_id
    into missing_boxes_table2
    from missing_boxes_table1 as bas
         inner join #vss on bas.account_number = #vss.account_number
;--49,481 boxes

select ccs.account_number
      ,card_subscriber_id
  into missing_boxes_table3
  from sk_prod.CUST_CARD_SUBSCRIBER_LINK as ccs
       inner join missing_boxes_table1 as bas on bas.account_number = ccs.account_number
       inner join sk_prod.cust_single_account_view as sav on ccs.account_number = sav.account_number
 where effective_to_dt = '9999-09-09'
   and cust_active_dtv = 1
;--90,098 boxes expected



select * from #table1 --accounts currently on panels
select * from #table2 --boxes currently on panels
select * from #table3 --boxes expected to be on panels





select count(distinct account_number) from #r2 where vesp=1 and cow>1
--27,345 should be in multi-box hhs, not single



--check that we only have 1 panel per account
select top 10 account_number,count(distinct panel_no) as cow
from sk_prod.vespa_subscriber_status
where result='Enabled'
group by account_number
having cow>1
;--0


grant select on missing_boxes_table1 to vespa_group_low_security;
grant select on missing_boxes_table2 to vespa_group_low_security;
grant select on missing_boxes_table3 to vespa_group_low_security;



