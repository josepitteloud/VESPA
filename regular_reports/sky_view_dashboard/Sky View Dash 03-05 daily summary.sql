-- Project Vespa: Sky View Dashboard Report Output 5: Daily Summary
select
    document_from_6AM
    ,log_count
    ,distinct_accounts
    ,distinct_boxes
from vespa_analysts.vespa_SVD_05_DailySummary_historics
order by document_from_6AM
;
