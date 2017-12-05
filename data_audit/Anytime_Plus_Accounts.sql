select distinct account_number 
into #tmp_acct_num
from sk_prod.cust_single_account_view
where cust_active_dtv = 1
and pty_country_code = 'GBR'

---lets sort active boxes

select * into #stb_active
from
(
    select  account_number
            ,service_instance_id
            ,active_box_flag
            ,box_installed_dt
            ,box_replaced_dt
            ,x_anytime_plus_enabled
            ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) active_flag
from sk_prod.cust_set_top_box) t
where active_flag = 1


---lets identify active boxes with anytime_plus_enabled flag

SELECT * INTO #anytime_plus_boxes
FROM
(select  a.* from #STB_ACTIVE a
where active_flag = 1
and box_replaced_dt = '9999-09-09'
and x_anytime_plus_enabled = 'Y') t


---so what do we have?

select * INTO #TST_FINAL_ACCTS
from
(select a.account_number, coalesce(count(distinct b.service_instance_id),0) boxes_count
FROM #tmp_acct_num A 
left outer join #anytime_plus_boxes B
on A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
group by a.account_number) t

select count(account_number) from #TST_FINAL_ACCTS
where boxes_count > 0

