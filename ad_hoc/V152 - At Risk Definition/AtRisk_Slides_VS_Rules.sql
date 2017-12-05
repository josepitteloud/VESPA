/*
At Risk rules against customer base on Nov 2012
*/

select
-- Unstable segments
case when value_seg_updated = 'UNSTABLE' then
                                case when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months')
                                and Box_Subscription in ('FDB','Sky Plus') and Sports1 in (0,1)  then 'Rule1'
                                when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months')
                                and Box_Subscription in ('FDB','Sky Plus') and Sports1 = 2  then 'Rule2'
                                when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months')
                                and Box_Subscription in ('FDB','Sky Plus') then 'Rule3'
                                when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months')
                                and Box_Subscription in ('HD')  then 'Rule4'
                                when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months') then 'Rule5'
                                when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs')
                                and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies','04 Other Prems')
                                and multiroom = 1  then 'Rule6'
                                when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs')
                                and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies','04 Other Prems')
                                 and multiroom = 0  then 'Rule7'
                                 when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs')
                                and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies','04 Other Prems') then 'Rule8'
                                when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs')
                                and tv_package in ('06 Ent Pack', '05 Ent Extra')  then 'Rule9'
                                when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs') then 'Rule10'
                                when tenure_group_latestactive in ('06 3-5 Yrs')
                                and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies','04 Other Prems')
                                 then 'Rule11'
                                when tenure_group_latestactive in ('06 3-5 Yrs') and tv_package in ('06 Ent Pack', '05 Ent Extra')  then 'Rule12'
                                when tenure_group_latestactive in ('06 3-5 Yrs') then 'Rule13'
                                when tenure_group_latestactive in ('07 5-10 Yrs','08 10+ Yrs') then 'Rule14'
                                else 'Unknown'
                                     end
                                else 'Unknown'
                                end as unstable
---------------------End of Contract Segment
,case when value_seg_updated <> 'UNSTABLE'
                            and tenure between 241 and 630 then
                                   case when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra')
                                        and lifestage in
                                        ('Young family no children <18','Young family with children <18','Young household with children <18',
                                        'Young singles/homesharers') and cable in ('Cable') then 'Rule1'
                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra')
                                        and lifestage in
                                        ('Young family no children <18','Young family with children <18','Young household with children <18',
                                        'Young singles/homesharers') and cable in ('Non-Cable') then 'Rule2'
                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra')
                                        and lifestage in
                                        ('Young family no children <18','Young family with children <18','Young household with children <18',
                                        'Young singles/homesharers') then 'Rule3'
                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra') and lifestage in
                                        ('Elderly family no children <18','Elderly single','Mature family no children <18',
                                        'Mature family with children <18','Mature household with children <18','Mature singles/homesharers',
                                        'Older family no children <18','Older family/household with children <18','Older single','Unknown')
                                        then 'Rule4'
                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra') then 'Rule5'
                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') and lifestage in
                                        ('Young family no children <18','Young family with children <18','Young household with children <18',
                                        'Young singles/homesharers') and cable in ('Cable') then 'Rule6'
                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') and lifestage in
                                        ('Young family no children <18','Young family with children <18','Young household with children <18',
                                        'Young singles/homesharers') and cable in ('Non-Cable') then 'Rule7'
                                         when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') and lifestage in
                                        ('Young family no children <18','Young family with children <18','Young household with children <18',
                                        'Young singles/homesharers') then 'Rule8'
                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') and lifestage in
                                        ('Elderly family no children <18','Elderly single','Mature family no children <18',
                                        'Mature family with children <18','Mature household with children <18','Mature singles/homesharers',
                                        'Older family no children <18','Older family/household with children <18','Older single','Unknown')
                                        then 'Rule9'
                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') then 'Rule10'
                                        when ta_saved_6m <= 0 then 'Rule11'
                                        when ta_saved_6m > 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems')  then 'Rule12'
                                        when ta_saved_6m > 0 and tv_package in ('06 Ent Pack','05 Ent Extra')  then 'Rule13'
                                        when ta_saved_6m > 0 then 'Rule14'
                                        else 'Unknown'
                                     end
                                else 'Unknown'
                                end as end_of_contract
------------------------------Long Tenure Segment----------------------
,case when value_seg_updated <> 'UNSTABLE'
                            and tenure > 630 then
                                   case when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems')
                                        and affluence in ('Very Low','Low','Mid Low','Unknown')
                                        and product_holding not in ('TV, SkyTalk and Line Rental and Broadband') then 'Rule1'
                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems')
                                        and affluence in ('Very Low','Low','Mid Low','Unknown')
                                        and product_holding in ('TV, SkyTalk and Line Rental and Broadband') then 'Rule2'
                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems')
                                        and affluence in ('Very Low','Low','Mid Low','Unknown') then 'Rule3'
                                        when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') and affluence in ('Very High','High','Mid High','Mid')
                                         then 'Rule4'
                                         when ta_saved_6m <= 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') then 'Rule5'
                                         when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra')
                                        and affluence in ('Very Low','Low','Mid Low','Unknown')
                                        and product_holding not in ('TV, SkyTalk and Line Rental and Broadband') then 'Rule6'
                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra')
                                        and affluence in ('Very Low','Low','Mid Low','Unknown')
                                        and product_holding in ('TV, SkyTalk and Line Rental and Broadband') then 'Rule7'
                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra')
                                        and affluence in ('Very Low','Low','Mid Low','Unknown') then 'Rule8'
                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra') and
                                        affluence in ('Very High','High','Mid High','Mid')
                                         then 'Rule9'
                                         when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack','05 Ent Extra')
                                         then 'Rule10'
                                          when ta_saved_6m <= 0 then 'Rule11'
                                        when ta_saved_6m > 0 and tv_package in ('01 Top Tier','02 Dual Sports','03 Dual Movies',
                                        '04 Other Prems') then 'Rule12'
                                        when ta_saved_6m > 0 and tv_package in ('06 Ent Pack','05 Ent Extra') then 'Rule13'
                                        when ta_saved_6m > 0 then 'Rule14'
                                        else 'Unknown'
                                     end
                                else 'Unknown'
                                end as long_tenure
        ,count(*) as Total_Volume
        ,sum(churn_flag) as Churn_Volume
        ,Churn_Volume*1.0/Total_Volume as Churn_Rate_3M
        ,Churn_Volume*4.0/Total_Volume as Churn_Rate_12M
from maitrap.bcg_base_20121130
group by unstable
        ,end_of_contract
        ,long_tenure
order by unstable
        ,end_of_contract
        ,long_tenure