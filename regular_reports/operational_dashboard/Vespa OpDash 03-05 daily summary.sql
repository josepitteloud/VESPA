-- Project Vespa: Operational Dashboard Report Output 5: Daily Summary
select
    document_from_9AM
    ,log_count
    ,distinct_accounts
    ,distinct_boxes
-- Don't often explicitly select things here, but those other columns are not properly built yet.
from vespa_analysts.vespa_OpDash_05_DailySummary_historics
order by document_from_9AM
;
