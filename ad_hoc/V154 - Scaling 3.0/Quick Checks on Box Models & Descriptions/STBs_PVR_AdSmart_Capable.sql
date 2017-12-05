/*
Identifying STBs by serinal number that are PVR and AdSmart capable
*/

select x_decoder_nds_number_prefix_4
        ,case 
            when x_box_type like 'Basic%' then 'No'
            when x_box_type like 'Sky+%' then 'Yes' 
         end as PVR
        ,case 
            when x_description like '%PVR6%' 
              or x_description like '%PVR5%' 
              or x_description like '%Samsung%PVR4%'
              or x_description like '%Pace%PVR4%' 
            then 'Yes' 
            else 'No' 
        end as AdSmart
        ,count(*)
from sk_prod.CUST_SET_TOP_BOX
where x_box_type is not null
group by x_decoder_nds_number_prefix_4,PVR,AdSmart
order by x_decoder_nds_number_prefix_4,PVR,AdSmart