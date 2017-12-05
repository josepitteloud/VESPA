/* TA Model */


insert  into TA_model_sourcedates
select  cast((right(attachments_table,6)) as integer)   as reference_
        ,base.thetable                                  as attachments_table
        ,right(attachments_table,2)                     as Account_Numbers_ending_in
        ,(
            select  max(utc_day_date)   
            from    sk_prod.vespa_calendar
            where   local_year_month = reference_
        )                                               as snapshot_date
        ,snapshot_date - 730    as _2_Years_Prior
        ,snapshot_date - 365    as _1_Year_Prior
        ,dateadd(month,-10,snapshot_date)               as _10_Months_Prior
        ,dateadd(month,-9,snapshot_date)                as _9_Months_Prior
        ,dateadd(month,-6,snapshot_date)                as _6_Months_Prior
        ,dateadd(month,-3,snapshot_date)                as _3_Months_Prior
        ,dateadd(month,-1,snapshot_date)                as _1_Months_Prior
        ,dateadd(month,1,snapshot_date)                 as _1_Months_Future
        ,dateadd(month,2,snapshot_date)                 as _2_Months_Future
        ,dateadd(month,3,snapshot_date)                 as _3_Months_Future
        ,dateadd(month,4,snapshot_date)                 as _4_Months_Future
        ,dateadd(month,5,snapshot_date)                 as _5_Months_Future
        ,dateadd(month,6,snapshot_date)                 as _6_Months_Future
from    (   
            select  creator||'.'||tname as thetable
            from    sys.syscatalog
            where   lower(tname) like 'view_attachments_%'
            and     creator = 'yarlagaddar'
        )   as base
        left join TA_model_sourcedates as tam
        on  base.thetable = tam.attachments_table 
where   tam.attachments_table is null;

commit;