drop table #stb_active;
drop table #active;

  select csi.account_number
        ,csi.service_instance_id
    into #active
    from sk_prod.cust_single_Account_view         as sav
         inner join sk_prod.cust_service_instance as csi on sav.account_number = csi.account_number
   where prod_latest_dtv_status_code in ('AB','AC','PC')
group by csi.account_number
        ,csi.service_instance_id
;--21,924,412

create hg index idx1 on #active(service_instance_id);

  select *
        ,cast(0 as bit) as vespa
        ,cast(0 as bit) as PS_flag
    into #stb_active
    from      (select stb.account_number
                     ,stb.service_instance_id
                     ,x_pvr_type
                     ,x_manufacturer
                     ,x_model_number
                     ,x_box_type
                     ,rank () over (partition by stb.service_instance_id order by ph_non_subs_link_sk desc) rank
                 from sk_prod.cust_Set_top_box as stb
                      inner join #active       as act on stb.service_instance_id = act.service_instance_id
                where box_replaced_dt = '9999-09-09') as sub
   where rank = 1
; --11329010

  update #stb_active as stb
     set vespa = 1
    from vespa_analysts.vespa_single_box_view as sbv
   where stb.service_instance_id = sbv.service_instance_id
     and status_vespa = 'Enabled'
     and panel='VESPA'
;--635324

  update #stb_active as stb
     set ps_flag = 1
    from sk_prod.CUST_SERVICE_INSTANCE as csi
   where stb.service_instance_id = csi.service_instance_id
     and si_service_instance_type = 'Primary DTV'
;--8,209,502 out of 11,329,010

--output 1
select x_pvr_type,x_manufacturer,count(1),sum(vespa),sum(ps_flag),sum(vespa * ps_flag)
  from #stb_active group by x_pvr_type,x_manufacturer
;

--output 2
select x_pvr_type,x_manufacturer,x_model_number,x_box_type,count(1),sum(ps_flag),sum(1-ps_flag)
  from #stb_active
  group by x_pvr_type,x_manufacturer,x_model_number,x_box_type
;


---
select distinct (account_number)
into #active
from sk_prod.cust_single_account_view
   where prod_latest_dtv_status_code in ('AB','AC','PC')
;--10,183,473

select distinct (service_instance_id)
  into #stb
  from sk_prod.cust_set_top_box as stb
       inner join #active as act on stb.account_number = act.account_number
 where box_installed_dt <= '2012-11-06'
   and box_replaced_dt  >  '2012-11-06'
;--13,469,195

select distinct (src_system_id)
  into #csi
  from sk_prod.cust_service_instance as csi
       inner join #active as act on csi.account_number = act.account_number
 where effective_to_dt = '9999-09-09'
;--11,740,963

create hg index idx1 on #stb(service_instance_id);
create hg index idx1 on #csi(src_system_id);


select count(distinct service_instance_id)
  from #stb
       inner join #csi on service_instance_id = src_system_id
;--11,329,010












