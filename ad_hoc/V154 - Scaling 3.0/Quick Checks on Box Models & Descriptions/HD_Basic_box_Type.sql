/*************************************************************************************

---
**************************************************************************************/

select top 10* from sk_prod.cust_set_top_box
select top 10* from sk_prod.cust_service_instance

select * from sk_prod.cust_set_top_box
where account_number = '200000846994'

select distinct account_number, x_box_type, x_manufacturer, x_description
into #temp_Boxtype_Descr
from sk_prod.cust_set_top_box
where x_box_type = 'Basic HD'
and  currency_code = 'GBP'
and box_installed_dt <= '2013-07-14'
and box_replaced_dt   > '2013-07-14'
--634,449 Row(s) affected


select top 100* from #temp_Boxtype_Descr

--Combining this to the boxtype based on account_number
select TB.account_number
      ,SEG.boxtype
into STB_Scaling_Var
from #temp_Boxtype_Descr TB
left join glasera.V154_account_numbers_variables SEG
on TB.account_number = SEG.account_number
--634,449 Row(s) affected

select top 1000* from STB_Scaling_Var


--Profiling Boxtytpe ---
select boxtype, count(distinct account_number) from STB_Scaling_Var
group by  boxtype

select boxtype, count(*) from STB_Scaling_Var
group by  boxtype

select top 10* from glasera.V154_account_numbers_variables_v2

x_box_type, x_manufacturer, x_description

----Pulling out box models and descriptions and merging this to the new scaling table

select account_number, x_box_type, x_manufacturer, x_description,service_instance_id
into Boxtype_Descr
from sk_prod.cust_set_top_box
where  currency_code = 'GBP'
and box_installed_dt <= '2013-07-14'
and box_replaced_dt   > '2013-07-14'
group by account_number, x_box_type, x_manufacturer, x_description,service_instance_id
--21,953,830 Row(s) affected

select src_system_id,si_external_identifier
into Sub_ID_Cust
from sk_prod.cust_service_instance
group by src_system_id,si_external_identifier
--75,473,830 Row(s) affected

select top 10* from sk_prod.cust_service_instance
select STB.account_number, STB.x_box_type, STB.x_manufacturer, STB.x_description,SI.si_external_identifier
into Boxtype_Descr_Sub_ID
from Boxtype_Descr STB
left join Sub_ID_Cust SI
on STB.service_instance_id = SI.src_system_id
--21,953,830 Row(s) affected

select STB.account_number
      ,STB.x_box_type
      ,STB.x_manufacturer
      ,STB.x_description
      ,STB.si_external_identifier
      ,NS.boxtype
      ,NS.boxtype_v2
      ,NS.sky_base_universe
into Final_Boxtype_Descr_Sub_ID
from glasera.V154_account_numbers_variables_v2 NS
left join Boxtype_Descr_Sub_ID STB
on NS.account_number = STB.account_number
--12,660,575 Row(s) affected

grant all on Final_Boxtype_Descr_Sub_ID to limac;
commit;
--Checks
select top 1000* from Final_Boxtype_Descr_Sub_ID
select top 10* from V154_boxtype_v3
select top 10* from glasera.V154_account_numbers_variables_v2

select * from glasera.V154_account_numbers_variables_v2
where account_number = '200000846994'

select top 100* from Vespa_Analysts.vespa_single_box_view
where account_number = 200000846994



