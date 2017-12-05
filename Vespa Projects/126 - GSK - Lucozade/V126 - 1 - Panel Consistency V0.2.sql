


--------------------------------------------------------------------------------
-- SET UP.
--------------------------------------------------------------------------------
-- create and populate variables
CREATE VARIABLE @var_period_start       datetime;
CREATE VARIABLE @var_period_end         datetime;

CREATE VARIABLE @var_sql                varchar(15000)

CREATE VARIABLE @var_cntr               smallint;

CREATE VARIABLE @i               integer;


SET @var_period_start           = '2012-07-02' --update to new period
SET @var_period_end             = '2012-07-07'


select @var_period_end

--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------






/*--------------------------------------------------------------------------------
-- SECTION 2: PART A -
--------------------------------------------------------------------------------

             A01 - IDENTIFY PRIMARY BOXES RETURNING DATA
             A02 - GET THE VIEWING DATA

--------------------------------------------------------------------------------*/


--------------------------------------------------------------------------------
--  A01 - identify boxes returning data over the period
--------------------------------------------------------------------------------


--identify boxes that returned data

IF object_id('the_boxes') IS NOT NULL DROP TABLE the_boxes

create table the_boxes (
    subscriber_id decimal(8) -- this is now added as we are interested in box level data - not household
    ,account_number varchar(20)
    ,cb_key_household bigint
    ,reporting_day varchar(8)
);


SET @var_sql = '

    insert into the_boxes
    select distinct(subscriber_id)
                ,account_number
                ,cb_key_household
                , ''##^^*^*##''
    from sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*##
     where (play_back_speed is null or play_back_speed = 2)
        and x_programme_viewed_duration > 0
        and Panel_id = 12 -- panel 4 is included due to date range covered - is this still needed?

';


-- loop though each days viewing logs to identify repeat data returners
SET @var_cntr = 0;
SET @i= datediff(dd,@var_period_start,@var_period_end);


WHILE @var_cntr <= @i
BEGIN
        EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(dateadd(day, @var_cntr, @var_period_start), 'yyyymmdd')))

        COMMIT

        SET @var_cntr = @var_cntr + 1
END




----------------------------------------------------------------------------------
-- Add a primary box flag as we are only interested in the primary box for this analysis - to keep it fair.
----------------------------------------------------------------------------------


alter table the_boxes
 add primary_flag as int default 0

 update the_boxes
 set rbs.primary_flag = case when sbv.ps_flag = 'P' then 1 else 0 end -- this can be adjusted to make it more thorough! -- ps olive
 from the_boxes as rbs
 left join vespa_analysts.vespa_single_box_view as sbv
 on rbs.subscriber_id = sbv.subscriber_id -- this should be done at subscriber_id level


select top 10 * from the_boxes

----------------------------------------------------------------------------------
-- Add a primary box flag as we are only interested in the primary box for this analysis - to keep it fair.
----------------------------------------------------------------------------------

-- lets get the dailt distribution
select distinct(reporting_day)
        , count(account_number) as boxes
        ,count(distinct(account_number)) as accounts
      --  ,count(distinct(case when primary_flag = 1 then account_number else null end)) as primary_accounts
        ,count(distinct(cb_key_household)) as households
from the_boxes
group by reporting_day
order by reporting_day



-- lets get the consistency



select distinct(cb_key_household) as cb_key_household
       ,max(primary_flag)
       ,count(distinct(cast(reporting_day as date))) as distinct_days
into consistency_table
from the_boxes
group by cb_key_household


-- now lets find the consistency of return;
alter table the_boxes
add consistency float



CREATE VARIABLE @days                   float;
set @days = 37

select @days


update consistency_table
        set consistency = cast(distinct_days as float)/cast(@days as float)


select * from consistency_table order by distinct_days

select distinct_days
        ,count(cb_key_household)
from consistency_table
group by distinct_days
order by distinct_days


select distinct(consistency) from consistency_table






