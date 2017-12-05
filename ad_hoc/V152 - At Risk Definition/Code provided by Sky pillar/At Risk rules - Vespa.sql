

------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------SET RULES FOR CUSCAN-----------------------------------------------------------

------------------Unstable Segment--------------

update bcg_base_20130411
set unstable_cuscan_rule1 =  case when value_seg_updated = 'UNSTABLE' then
                                case when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months')
                                and Box_Subscription in ('FDB','Sky Plus') and Sports1 = 0 or Sports1 = 1  then 'Rule1'
                                when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months')
                                and Box_Subscription in ('FDB','Sky Plus') and Sports1 = 2  then 'Rule2'
                                when tenure_group_latestactive in ('01 1-3 months','02 4-6 months','03 6-12 months')
                                and Box_Subscription in ('HD')  then 'Rule3'
                                when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs')
                                and mix_pack in ('05 Ent Extra') and multiroom = 0  then 'Rule4'
                                when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs')
                                and mix_pack in ('05 Ent Extra') and multiroom = 1  then 'Rule5'
                                when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs')
                                and mix_pack in ('06 Ent Pack') and cs_call_6m <= 0  then 'Rule6'
                                when tenure_group_latestactive in ('04 1-2 years','05 2-3 Yrs')
                                and mix_pack in ('06 Ent Pack') and cs_call_6m > 0  then 'Rule7'
                                when tenure_group_latestactive in ('06 3-5 Yrs')
                                and mix_pack in ('05 Ent Extra') and late_paid_bills_6m <= 0  then 'Rule8'
                                when tenure_group_latestactive in ('06 3-5 Yrs')
                                and mix_pack in ('05 Ent Extra') and late_paid_bills_6m > 0  then 'Rule9'
                                when tenure_group_latestactive in ('06 3-5 Yrs') and mix_pack in ('06 Ent Pack')  then 'Rule10'
                                when tenure_group_latestactive in ('07 5-10 Yrs','08 10+ Yrs')
                                and Box_Subscription in ('FDB','Sky Plus') and broadband = 0  then 'Rule11'
                                when tenure_group_latestactive in ('07 5-10 Yrs','08 10+ Yrs')
                                and Box_Subscription in ('FDB','Sky Plus') and broadband = 1  then 'Rule12'
                                when tenure_group_latestactive in ('07 5-10 Yrs','08 10+ Yrs') and Box_Subscription in ('HD')  then 'Rule13'
                                else 'Unknown'

                                     end
                                else 'Unknown'
                                end;

------------------------9218821

---------------------End of Contract Segment

update bcg_base_20130411
set EOC_cuscan_rule1 =  case when value_seg_updated <> 'UNSTABLE'
                            and tenure between 241 and 630 then
                                   case when ta_saved_6m <= 0 and mix_pack in ('05 Ent Extra') and cable in ('Cable') then 'Rule1'
                                        when ta_saved_6m <= 0 and mix_pack in ('05 Ent Extra') and cable in ('Non-Cable') and count_upgrades <= 0 then 'Rule2'
                                        when ta_saved_6m <= 0 and mix_pack in ('05 Ent Extra') and cable in ('Non-Cable') and count_upgrades > 0 then 'Rule3'
                                        when ta_saved_6m <= 0 and mix_pack in ('06 Ent Pack') and cable in ('Cable') and lifestage in
                                        ('Young family no children <18','Young family with children <18','Young household with children <18','Young singles/homesharers') then 'Rule4'
                                        when ta_saved_6m <= 0 and mix_pack in ('06 Ent Pack') and cable in ('Cable') and lifestage in
                                        ('Elderly family no children <18','Elderly single','Mature family no children <18','Mature family with children <18','Mature household with children <18','Mature singles/homesharers','Older family no children <18','Older family/household with children <18','Older single','Unknown') and NLP <= 0 then 'Rule5'
                                        when ta_saved_6m <= 0 and mix_pack in ('06 Ent Pack') and cable in ('Cable') and lifestage in
                                        ('Elderly family no children <18','Elderly single','Mature family no children <18','Mature family with children <18','Mature household with children <18','Mature singles/homesharers','Older family no children <18','Older family/household with children <18','Older single','Unknown') and NLP > 0 then 'Rule6'
                                        when ta_saved_6m <= 0 and mix_pack in ('06 Ent Pack') and cable in ('Non-Cable') then 'Rule7'
                                        when ta_saved_6m > 0 and mix_pack in ('05 Ent Extra') then 'Rule8'
                                        when ta_saved_6m > 0 and mix_pack in ('06 Ent Pack') then 'Rule9'
                                        else 'Unknown'


                                     end
                                else 'Unknown'
                                end;

----------------------------------9218821



------------------------------Long Tenure Segment----------------------



update bcg_base_20130411
set longtenure_cuscan_rule1 =  case when value_seg_updated <> 'UNSTABLE'
                            and tenure > 630 then
                                   case when ta_saved_6m <= 0 and tv_package in ('01 Top Tier') or tv_package IS null then 'Rule1'
                                        when ta_saved_6m <= 0 and tv_package in ('02 Dual Sports','03 Dual Movies') then 'Rule2'
                                        when ta_saved_6m <= 0 and tv_package in ('04 Other Prems','05 Ent Extra') then 'Rule3'
                                        when ta_saved_6m <= 0 and tv_package in ('06 Ent Pack') then 'Rule4'
                                        when ta_saved_6m > 0 then 'Rule5'
                                        else 'Unknown'
                                     end
                                else 'Unknown'
                                end;
----------------------9218821



