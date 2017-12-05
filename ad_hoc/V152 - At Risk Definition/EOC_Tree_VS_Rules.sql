/*
Mapping SQL rules to the classification tree presented in slide 2 of deck:

New At Risk proposed segments - Cuscan and TA
23 April 2013

*/

------------------ End of Contract Segment -------------------------------------------------------- 
-- update bcg_base_20121130
--    set EOC_cuscan_rule1 = case when value_seg_updated <> 'UNSTABLE'
--                                 and tenure between 241 and 630 then
--                                    case when ta_saved_6m <= 0 and mix_pack in ('05 Ent Extra') and cable in ('Cable')                             then 'Rule1'
--                                         when ta_saved_6m <= 0 and mix_pack in ('05 Ent Extra') and cable in ('Non-Cable') and count_upgrades <= 0 then 'Rule2'
--                                         when ta_saved_6m <= 0 and mix_pack in ('05 Ent Extra') and cable in ('Non-Cable') and count_upgrades > 0  then 'Rule3'
--                                         when ta_saved_6m <= 0 and mix_pack in ('06 Ent Pack')  and cable in ('Cable')     and lifestage in ('Young family no children <18','Young family with children <18','Young household with children <18','Young singles/homesharers') then 'Rule4'
--                                         when ta_saved_6m <= 0 and mix_pack in ('06 Ent Pack')  and cable in ('Cable')     and lifestage in ('Elderly family no children <18','Elderly single','Mature family no children <18','Mature family with children <18','Mature household with children <18','Mature singles/homesharers','Older family no children <18','Older family/household with children <18','Older single','Unknown') and NLP <= 0 then 'Rule5'
--                                         when ta_saved_6m <= 0 and mix_pack in ('06 Ent Pack')  and cable in ('Cable')     and lifestage in ('Elderly family no children <18','Elderly single','Mature family no children <18','Mature family with children <18','Mature household with children <18','Mature singles/homesharers','Older family no children <18','Older family/household with children <18','Older single','Unknown') and NLP > 0  then 'Rule6'
--                                         when ta_saved_6m <= 0 and mix_pack in ('06 Ent Pack')  and cable in ('Non-Cable')                         then 'Rule7'
--                                         when ta_saved_6m >  0 and mix_pack in ('05 Ent Extra')                                                    then 'Rule8'
--                                         when ta_saved_6m >  0 and mix_pack in ('06 Ent Pack')                                                     then 'Rule9'
--                                         else 'Unknown'
--                                      end
--                                 else 'Unknown'
--                                 end;

--- Final segments from slides
select case 
            when EOC_cuscan_rule1 in ('Rule4','Rule5','Rule6','Rule9') then EOC_cuscan_rule1
            when EOC_cuscan_rule1 in ('Rule8') and tv_package in('06 Ent Pack','05 Ent Extra') then 'Rule8a'
            when EOC_cuscan_rule1 in ('Rule7') and tv_package in('06 Ent Pack','05 Ent Extra') and Product_holding = 'TV Only' then 'Rule7a'
            when EOC_cuscan_rule1 in ('Rule1') and tv_package in('06 Ent Pack','05 Ent Extra') and Product_holding = 'TV Only' then 'Rule1a'
            else null
         end as Cuscan_EOC
        ,count(*) as total_volume
        ,sum(churn_flag) as churn_volume
        ,churn_volume*1.0/total_volume as churn_rate
from maitrap.bcg_base_20121130
where Cuscan_EOC is not null
group by Cuscan_EOC
order by Cuscan_EOC
