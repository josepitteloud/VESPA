
----Pulling out box models and descriptions and merging this to the new scaling table
--Checks select top 10* from sk_prod.cust_set_top_box
select account_number
      ,x_box_type
      ,x_description
      ,x_decoder_nds_number_prefix_4
      ,service_instance_id
into Boxtype_Descr_Serial_no
from sk_prod.cust_set_top_box
where  currency_code = 'GBP'
and box_installed_dt <= '2013-07-14'
and box_replaced_dt   > '2013-07-14'
group by account_number
        ,x_box_type
        ,x_description
        ,x_decoder_nds_number_prefix_4
        ,service_instance_id
--21,954,851 Row(s) affected
select top 100* from Boxtype_Descr_Serial_no

--Note that src_system is same as service_instance_id and it's more populated. Also si_external_identifier is same as subscriber_id
select src_system_id,si_external_identifier
into Sub_ID_Cust
from sk_prod.cust_service_instance
group by src_system_id,si_external_identifier
--75,974,934 Row(s) affected

--Joining the two tables  above together in other to have the si_external_identifier (which is subscriber_id)

select STB.account_number
      ,STB.x_box_type
      ,STB.x_description
      ,STB.x_decoder_nds_number_prefix_4
      ,SI.si_external_identifier
into Boxtype_Descr_Serial_no_Sub_ID
from Boxtype_Descr_Serial_no STB
left join Sub_ID_Cust SI
on STB.service_instance_id = SI.src_system_id
--21,954,851 Row(s) affected
select top 10* from glasera.V154_account_numbers_variables_v2

--Joining the above tables to the new scaling table based on account_number
select top 10* from Final_Boxtype_Descr_Sub_ID
select STB.account_number
      ,STB.x_box_type
      ,STB.x_description
      ,STB.x_decoder_nds_number_prefix_4
      ,STB.si_external_identifier
      ,NS.sky_base_universe
      ,NS.pvr
into Box_Serial_PVR_Adsmartable
from glasera.V154_account_numbers_variables_v2 NS
left join Boxtype_Descr_Serial_no_Sub_ID STB
on NS.account_number = STB.account_number
--12,661,459 Row(s) affected
select top 10* from glasera.V154_account_numbers_variables_v2
select top 1000* from Box_Serial_PVR_Adsmartable

grant all on Final_Boxtype_Descr_Sub_ID to limac;
commit;

grant all on Box_Serial_PVR_Adsmartable to limac;
commit;
igonorp.Box_Serial_PVR_Adsmartable

--Deciding which box type has PVR capabilities ---

select account_number
      ,max(case when x_box_type like 'Sky+%' then 'Yes' else 'No' end) as PVR
into   Final_Boxtype_Descr_Sub_ID_PVR
from   Final_Boxtype_Descr_Sub_ID
group by account_number
--9,222,293 Row(s) affected

--Joining the above table (Final_Boxtype_Descr_Sub_ID_PVR) back to Final_Boxtype_Descr_Sub_ID
select FB.*
      ,FBP.PVR
into Final_Boxtype_Descr_Sub_ID_PVR_Combined
from Final_Boxtype_Descr_Sub_ID FB
left join Final_Boxtype_Descr_Sub_ID_PVR FBP
on FB.account_number = FBP.account_number
--12,660,575 Row(s) affected

***************************************************************--Checks..Checks....Checks------------------
select top 1000* from Final_Boxtype_Descr_Sub_ID_PVR_Combined
where account_number is not null

select top 10000 account_number,x_box_type,boxtype, PVR
from Final_Boxtype_Descr_Sub_ID_PVR_Combined
where  account_number = '200001524855'
--where account_number is not null
order by account_number
--where  account_number = '200001524855'

select count(A.account_number)
from glasera.V154_account_numbers_variables_v2 A
left join Final_Boxtype_Descr_Sub_ID_PVR_Combined B
on A.account_number = B.account_number
where B.account_number is null
--184,036

grant all on Final_Boxtype_Descr_Sub_ID_PVR_Combined to glasera
select top 10* from Final_Boxtype_Descr_Sub_ID_PVR_Combined
where account_number is not null

select boxtype, count(*)
from glasera.V154_account_numbers_variables_v2
group by boxtype

--Checks (Discuss this with Claudio)
select * from glasera.V154_account_numbers_variables_v2
--group by x_box_type, x_description
where account_number = '200001524855'


select x_box_type, x_description from Boxtype_Descr
--group by x_box_type, x_description
where account_number = '200001524855'

SELECT *
FROM Final_Boxtype_Descr_Sub_ID
WHERE si_external_identifier IN
(
      SELECT si_external_identifier
      FROM Final_Boxtype_Descr_Sub_ID
      GROUP BY si_external_identifier
      HAVING COUNT(*) > 1
)
ORDER BY si_external_identifier

200000000768
200000000768
select top 1000* from Final_Boxtype_Descr_Sub_ID
select top 10* from V154_boxtype_v3
select top 10* from glasera.V154_account_numbers_variables_v2
select * from glasera.V154_account_numbers_variables_v2
where account_number = '200000846994'


select * from glasera.V154_account_numbers_variables_v2
where account_number = '200000847620'

select * from Vespa_Analysts.vespa_single_box_view
where account_number = '200000847216'

select * from Final_Boxtype_Descr_Sub_ID
where account_number = '200000847216'

select * from sk_prod.cust_set_top_box
where account_number = '200000847216'


select boxtype from glasera.V154_account_numbers_variables_v2
--group by x_box_type, x_description
where account_number = '200001524855'

select       x_box_type, boxtype,  count(*)
        from Final_Boxtype_Descr_Sub_ID_PVR_Combined
       where lower(x_box_type) like '%basic%' and lower(boxtype) like '%hd%
    group by x_box_type, boxtype

select top 20 *
        from Final_Boxtype_Descr_Sub_ID_PVR_Combined

--checks
select x_box_type, x_description from Boxtype_Descr
where account_number = '200001524855'
group by x_box_type, x_description


select x_box_type, x_description from sk_prod.cust_set_top_box
--group by x_box_type, x_description
where account_number = '200001524855'

select x_box_type, count(*) from sk_prod.cust_set_top_box
where account_number = '200001524855'
group by x_box_type

select boxtype, PVR, count(*) from glasera.V154_account_numbers_variables_v2
group by boxtype, PVR
select top 10* from glasera.V154_account_numbers_variables_v2

