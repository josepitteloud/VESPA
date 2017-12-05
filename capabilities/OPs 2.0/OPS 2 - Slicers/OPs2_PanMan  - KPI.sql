-- Sky Base - Account population
select count (account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW

-- DP - Account population
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'

-- DP - Panel accounts with acceptable reporting
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and reporting_quality >=0.9

-- DP - Panel accounts reporting unreliably
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and reporting_quality <0.9

-- DP - Panel accounts with zero reporting
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and Num_logs_sent_7D is null

-- DP - Panel accounts only recently activated
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and datediff(day, SBV.enablement_date, SBV.weekending)<15

-- Alt 6 - Account population
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'ALT6'

-- Alt 6 - Panel accounts with acceptable reporting
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'ALT6'
and reporting_quality >=0.9

-- Alt 6 - Panel accounts reporting unreliably
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'ALT6'
and reporting_quality <0.9

-- Alt 6 - Panel accounts with zero reporting
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'ALT6'
and Num_logs_sent_7D is null

-- Alt 6 - Panel accounts only recently activated
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'ALT6'
and datediff(day, SBV.enablement_date, SBV.weekending)<15

-- Alt 7 - Account population
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'ALT7'

-- Alt 7 - Panel accounts with acceptable reporting
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'ALT7'
and reporting_quality >=0.9

-- Alt 7 - Panel accounts reporting unreliably
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'ALT7'
and reporting_quality <0.9

-- Alt 7 - Panel accounts with zero reporting
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'ALT7'
and Num_logs_sent_7D is null

-- Alt 7 - Panel accounts only recently activated
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'ALT7'
and datediff(day, SBV.enablement_date, SBV.weekending)<15

-- DP  Multi Box - Account population
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and Num_boxes > 1

-- DP  Multi Box - Panel accounts with acceptable reporting
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and reporting_quality >=0.9
and Num_boxes > 1

-- DP  Multi Box - Panel accounts reporting unreliably
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and reporting_quality <0.9
and Num_boxes > 1

-- DP  Multi Box - Panel accounts with zero reporting
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and Num_logs_sent_7D is null
and Num_boxes > 1

-- DP  Multi Box - Panel accounts only recently activated
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and datediff(day, SBV.enablement_date, SBV.weekending)<15
and Num_boxes > 1

-- DP  Single Box - Account population
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and Num_boxes = 1

-- DP  Single Box - Panel accounts with acceptable reporting
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and reporting_quality >=0.9
and Num_boxes = 1

-- DP  Single Box - Panel accounts reporting unreliably
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and reporting_quality <0.9
and Num_boxes = 1

-- DP  Single Box - Panel accounts with zero reporting
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and Num_logs_sent_7D is null
and Num_boxes = 1

-- DP  Single Box - Panel accounts only recently activated
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and datediff(day, SBV.enablement_date, SBV.weekending)<15
and Num_boxes = 1

--Universe - Sky Base HH
select count (account_number)
       ,case when Num_boxes = 1 then 'A) Single Box HH'
             when Num_boxes > 1 then 'B) Multiple Box HH'
       else 'Unknown' end as Universe
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW
group by Universe

--ISBA TV Region - Sky Base HH
select count (account_number), ssl.isba_tv_region
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
group by ssl.isba_tv_region

-- ISBA TV Region - DP HH
select count (distinct SAV.account_number), ssl.isba_tv_region
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
group by ssl.isba_tv_region

-- ISBA TV Region - Acceptable Reporting DP HH
select count (distinct SAV.account_number), ssl.isba_tv_region
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and reporting_quality >=0.9
group by ssl.isba_tv_region

-- ISBA TV Region - Unreliable DP HH
select count (distinct SAV.account_number), ssl.isba_tv_region
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and reporting_quality <0.9
group by ssl.isba_tv_region

-- ISBA TV Region - Zero Reporting DP HH
select count (distinct SAV.account_number), ssl.isba_tv_region
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and Num_logs_sent_7D is null
group by ssl.isba_tv_region

-- ISBA TV Region - Recently Enabled DP HH
select count (distinct SAV.account_number), ssl.isba_tv_region
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and datediff(day, SBV.enablement_date, SBV.weekending)<15
group by ssl.isba_tv_region

-- HH Composition - Sky Base HH
select count (account_number), ssl.hhcomposition
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
group by ssl.hhcomposition

-- HH Composition - DP HH
select count (distinct SAV.account_number), ssl.hhcomposition
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
group by ssl.hhcomposition

-- HH Composition - Acceptable Reporting DP HH
select count (distinct SAV.account_number), ssl.hhcomposition
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and reporting_quality >=0.9
group by ssl.hhcomposition

-- HH Composition - Unreliable DP HH
select count (distinct SAV.account_number), ssl.hhcomposition
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and reporting_quality <0.9
group by ssl.hhcomposition

-- HH Composition - Zero Reporting DP HH
select count (distinct SAV.account_number), ssl.hhcomposition
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and Num_logs_sent_7D is null
group by ssl.hhcomposition

-- HH Composition - Recently Enabled DP HH
select count (distinct SAV.account_number), ssl.hhcomposition
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and datediff(day, SBV.enablement_date, SBV.weekending)<15
group by ssl.hhcomposition

-- Tenure - Sky Base HH
select count (account_number), ssl.tenure
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
group by ssl.tenure

-- Tenure - DP HH
select count (distinct SAV.account_number), ssl.tenure
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
group by ssl.tenure

-- Tenure - Acceptable Reporting DP HH
select count (distinct SAV.account_number), ssl.tenure
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and reporting_quality >=0.9
group by ssl.tenure

-- Tenure - Unreliable DP HH
select count (distinct SAV.account_number), ssl.tenure
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and reporting_quality <0.9
group by ssl.tenure

-- Tenure - Zero Reporting DP HH
select count (distinct SAV.account_number), ssl.tenure
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and Num_logs_sent_7D is null
group by ssl.tenure

-- Tenure - Recently Enabled DP HH
select count (distinct SAV.account_number), ssl.tenure
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and datediff(day, SBV.enablement_date, SBV.weekending)<15
group by ssl.tenure

-- Package - Sky Base HH
select count (account_number), ssl.package
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
group by ssl.package

-- Package - DP HH
select count (distinct SAV.account_number), ssl.package
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
group by ssl.package

-- Package - Acceptable Reporting DP HH
select count (distinct SAV.account_number), ssl.package
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and reporting_quality >=0.9
group by ssl.package

-- Package - Unreliable DP HH
select count (distinct SAV.account_number), ssl.package
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and reporting_quality <0.9
group by ssl.package

-- Package - Zero Reporting DP HH
select count (distinct SAV.account_number), ssl.package
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and Num_logs_sent_7D is null
group by ssl.package

-- Package - Recently Enabled DP HH
select count (distinct SAV.account_number), ssl.package
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and datediff(day, SBV.enablement_date, SBV.weekending)<15
group by ssl.package

-- Box Type - Sky Base HH
select count (account_number), ssl.boxtype
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
group by ssl.boxtype

-- Box Type - DP HH
select count (distinct SAV.account_number), ssl.boxtype
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
group by ssl.boxtype

-- Box Type - Acceptable Reporting DP HH
select count (distinct SAV.account_number), ssl.boxtype
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and reporting_quality >=0.9
group by ssl.boxtype

-- Box Type - Unreliable DP HH
select count (distinct SAV.account_number), ssl.boxtype
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and reporting_quality <0.9
group by ssl.boxtype

-- Box Type - Zero Reporting DP HH
select count (distinct SAV.account_number), ssl.boxtype
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and Num_logs_sent_7D is null
group by ssl.boxtype

-- Box Type - Recently Enabled DP HH
select count (distinct SAV.account_number), ssl.boxtype
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl
on sav.scaling_segment_ID = ssl.scaling_segment_ID
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and datediff(day, SBV.enablement_date, SBV.weekending)<15
group by ssl.boxtype

-- Value Segment - Sky Base HH
select count (SAV.account_number), ssl.value_seg
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
inner join sk_prod.VALUE_SEGMENTS_DATA as ssl
on sav.account_number = ssl.account_number
group by ssl.value_seg

-- Value Segment - DP HH
select count (distinct SAV.account_number), ssl.value_seg
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join sk_prod.VALUE_SEGMENTS_DATA as ssl
on sav.account_number = ssl.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
group by ssl.value_seg

-- Value Segment - Acceptable Reporting DP HH
select count (distinct SAV.account_number), ssl.value_seg
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join sk_prod.VALUE_SEGMENTS_DATA as ssl
on sav.account_number = ssl.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and reporting_quality >=0.9
group by ssl.value_seg

-- Value Segment - Unreliable DP HH
select count (distinct SAV.account_number), ssl.value_seg
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join sk_prod.VALUE_SEGMENTS_DATA as ssl
on sav.account_number = ssl.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and reporting_quality <0.9
group by ssl.value_seg

-- Value Segment - Zero Reporting DP HH
select count (distinct SAV.account_number), ssl.value_seg
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join sk_prod.VALUE_SEGMENTS_DATA as ssl
on sav.account_number = ssl.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and Num_logs_sent_7D is null
group by ssl.value_seg

-- Value Segment - Recently Enabled DP HH
select count (distinct SAV.account_number), ssl.value_seg
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join sk_prod.VALUE_SEGMENTS_DATA as ssl
on sav.account_number = ssl.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and datediff(day, SBV.enablement_date, SBV.weekending)<15
group by ssl.value_seg

--MOSAIC Segment - Sky Base HH
select count (distinct SAV.account_number), coalesce(con.h_mosaic_uk_group, 'U') as MOSAIC_segment
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
inner join sk_prod.CUST_SINGLE_ACCOUNT_VIEW as CSAV
on sav.account_number = CSAV.account_number
inner join sk_prod.EXPERIAN_CONSUMERVIEW as con
on csav.cb_key_household = con.cb_key_household
group by MOSAIC_segment

-- MOSAIC Segment - DP HH
select count (distinct SAV.account_number), coalesce(con.h_mosaic_uk_group, 'U') as MOSAIC_segment
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join sk_prod.CUST_SINGLE_ACCOUNT_VIEW as CSAV
on sav.account_number = CSAV.account_number
inner join sk_prod.EXPERIAN_CONSUMERVIEW as con
on csav.cb_key_household = con.cb_key_household
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
group by MOSAIC_segment

-- MOSAIC Segment - Acceptable Reporting DP HH
select count (distinct SAV.account_number), coalesce(con.h_mosaic_uk_group, 'U') as MOSAIC_segment
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join sk_prod.CUST_SINGLE_ACCOUNT_VIEW as CSAV
on sav.account_number = CSAV.account_number
inner join sk_prod.EXPERIAN_CONSUMERVIEW as con
on csav.cb_key_household = con.cb_key_household
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and reporting_quality >=0.9
group by MOSAIC_segment

-- MOSAIC Segment - Unreliable DP HH
select count (distinct SAV.account_number), coalesce(con.h_mosaic_uk_group, 'U') as MOSAIC_segment
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join sk_prod.CUST_SINGLE_ACCOUNT_VIEW as CSAV
on sav.account_number = CSAV.account_number
inner join sk_prod.EXPERIAN_CONSUMERVIEW as con
on csav.cb_key_household = con.cb_key_household
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and reporting_quality <0.9
group by MOSAIC_segment

-- MOSAIC Segment - Zero Reporting DP HH
select count (distinct SAV.account_number), coalesce(con.h_mosaic_uk_group, 'U') as MOSAIC_segment
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join sk_prod.CUST_SINGLE_ACCOUNT_VIEW as CSAV
on sav.account_number = CSAV.account_number
inner join sk_prod.EXPERIAN_CONSUMERVIEW as con
on csav.cb_key_household = con.cb_key_household
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and datediff(day, SBV.enablement_date, SBV.weekending)<15
group by MOSAIC_segment

-- MOSAIC Segment - Recently Enabled DP HH
select count (distinct SAV.account_number), coalesce(con.h_mosaic_uk_group, 'U') as MOSAIC_segment
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join sk_prod.CUST_SINGLE_ACCOUNT_VIEW as CSAV
on sav.account_number = CSAV.account_number
inner join sk_prod.EXPERIAN_CONSUMERVIEW as con
on csav.cb_key_household = con.cb_key_household
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and Num_logs_sent_7D is null
group by MOSAIC_segment

--FSS Segment - Sky Base HH
select count (distinct SAV.account_number), coalesce(con.h_fss_group, 'U')          as Financial_strategy_segment
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
inner join sk_prod.CUST_SINGLE_ACCOUNT_VIEW as CSAV
on sav.account_number = CSAV.account_number
inner join sk_prod.EXPERIAN_CONSUMERVIEW as con
on csav.cb_key_household = con.cb_key_household
group by Financial_strategy_segment

-- FSS Segment - DP HH
select count (distinct SAV.account_number), coalesce(con.h_fss_group, 'U')          as Financial_strategy_segment
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join sk_prod.CUST_SINGLE_ACCOUNT_VIEW as CSAV
on sav.account_number = CSAV.account_number
inner join sk_prod.EXPERIAN_CONSUMERVIEW as con
on csav.cb_key_household = con.cb_key_household
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
group by Financial_strategy_segment

-- FSSt - Acceptable Reporting DP HH
select count (distinct SAV.account_number), coalesce(con.h_fss_group, 'U')          as Financial_strategy_segment
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join sk_prod.CUST_SINGLE_ACCOUNT_VIEW as CSAV
on sav.account_number = CSAV.account_number
inner join sk_prod.EXPERIAN_CONSUMERVIEW as con
on csav.cb_key_household = con.cb_key_household
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and reporting_quality >=0.9
group by Financial_strategy_segment

-- FSS Segment - Unreliable DP HH
select count (distinct SAV.account_number), coalesce(con.h_fss_group, 'U')          as Financial_strategy_segment
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join sk_prod.CUST_SINGLE_ACCOUNT_VIEW as CSAV
on sav.account_number = CSAV.account_number
inner join sk_prod.EXPERIAN_CONSUMERVIEW as con
on csav.cb_key_household = con.cb_key_household
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and reporting_quality <0.9
group by Financial_strategy_segment

-- FSS Segment - Zero Reporting DP HH
select count (distinct SAV.account_number), coalesce(con.h_fss_group, 'U')          as Financial_strategy_segment
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join sk_prod.CUST_SINGLE_ACCOUNT_VIEW as CSAV
on sav.account_number = CSAV.account_number
inner join sk_prod.EXPERIAN_CONSUMERVIEW as con
on csav.cb_key_household = con.cb_key_household
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and datediff(day, SBV.enablement_date, SBV.weekending)<15
group by Financial_strategy_segment

-- FSS Segment - Recently Enabled DP HH
select count (distinct SAV.account_number), coalesce(con.h_fss_group, 'U')          as Financial_strategy_segment
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join sk_prod.CUST_SINGLE_ACCOUNT_VIEW as CSAV
on sav.account_number = CSAV.account_number
inner join sk_prod.EXPERIAN_CONSUMERVIEW as con
on csav.cb_key_household = con.cb_key_household
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and Num_logs_sent_7D is null
group by Financial_strategy_segment

-- Sky Go - Sky Base HH
select count (SAV.account_number)
from vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
inner join sk_prod.SKY_PLAYER_USAGE_DETAIL as ssl
on sav.account_number = SSL.account_number
and activity_dt >= '2011-08-18'

-- Sky Go - DP HH
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join sk_prod.SKY_PLAYER_USAGE_DETAIL as ssl
on sav.account_number = SSL.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and activity_dt >= '2011-08-18'

-- Sky Go - Acceptable Reporting DP HH
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join sk_prod.SKY_PLAYER_USAGE_DETAIL as ssl
on sav.account_number = SSL.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and reporting_quality >=0.9
and activity_dt >= '2011-08-18'

-- Sky Go - Unreliable DP HH
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join sk_prod.SKY_PLAYER_USAGE_DETAIL as ssl
on sav.account_number = SSL.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and reporting_quality <0.9
and activity_dt >= '2011-08-18'

-- Sky Go - Acceptable Reporting DP HH
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join sk_prod.SKY_PLAYER_USAGE_DETAIL as ssl
on sav.account_number = SSL.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and Num_logs_sent_7D is null
and activity_dt >= '2011-08-18'

-- Sky Go - Recently Enabled DP HH
select count (distinct SAV.account_number)
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on SBV.account_number = SAV.account_number
inner join sk_prod.SKY_PLAYER_USAGE_DETAIL as ssl
on sav.account_number = SSL.account_number
where SBV.status_vespa = 'Enabled'
and SBV.panel = 'VESPA'
and datediff(day, SBV.enablement_date, SBV.weekending)<15
and activity_dt >= '2011-08-18'

