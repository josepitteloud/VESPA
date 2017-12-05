
/************ PART A01 : GETTING DATA INTO THE DATABASE ***********/

-- no keys or indices, we'll worry about that after we format the data

drop table  V068_ad_hoc_box_reporting_raw;

create table V068_ad_hoc_box_reporting_raw (
    panel               int
    ,subscriber_id      bigint
    ,box_type           varchar(10)
    ,enablement_date    varchar(12)
    ,dr_040712          varchar(1)
    ,dr_050712          varchar(1)
    ,dr_060712          varchar(1)
    ,dr_070712          varchar(1)
    ,dr_080712          varchar(1)
    ,dr_090712          varchar(1)
    ,dr_100712          varchar(1)
    ,dr_110712          varchar(1)
    ,dr_120712          varchar(1)
    ,dr_130712          varchar(1)
    ,dr_140712          varchar(1)
    ,dr_150712          varchar(1)
    ,dr_160712          varchar(1)
    ,dr_170712          varchar(1)
    ,dr_180712          varchar(1)
    ,dr_190712          varchar(1)
    ,dr_200712          varchar(1)
    ,dr_210712          varchar(1)
    ,dr_220712          varchar(1)
    ,dr_230712          varchar(1)
    ,dr_240712          varchar(1)
    ,most_recent_return varchar(20)
);

commit;
go

-- cool, importing stuff then:

input into stafforr.V068_ad_hoc_box_reporting_raw
from 'D:\\Vespa\\Panel enablement\\panel12_from_040712.csv'
format ascii;
commit;
go

input into stafforr.V068_ad_hoc_box_reporting_raw
from 'D:\\Vespa\\Panel enablement\\panel6_from_040712.csv'
format ascii;
commit;
go

input into stafforr.V068_ad_hoc_box_reporting_raw
from 'D:\\Vespa\\Panel enablement\\panel7_from_040712.csv'
format ascii;
commit;
go

-- ^^ - restart goes to here!

-- We'll process into table structure later!

/************ PART A02 : TURNING IT INTO USEFUL TABLE STRUCTURE ***********/

-- Oh hey it's later. But first, some iport control totals:

select panel, count(1) as boxes
from stafforr.V068_ad_hoc_box_reporting_raw
group by panel;
/*
12,1148999
,6          <- these guys are the header items at the top of each file
7,428084
6,428031
*/

-- Okay, sweet.

delete from stafforr.V068_ad_hoc_box_reporting_raw
where panel is null;
commit;
go

-- OK, so we need a different columns for panel enablement date that's actually a date,
-- plus we need an account number and a reporting metric field too.

alter table stafforr.V068_ad_hoc_box_reporting_raw
    add account_number          varchar(20)
    ,add enable_date            date
    ,add reporting_score        float
    ,add reporting_quality      float
;

create unique index fake_pk on stafforr.V068_ad_hoc_box_reporting_raw (subscriber_id);
commit;
go

-- All the profiling stuff we're pulling from 
update V068_ad_hoc_box_reporting_raw
set V068_ad_hoc_box_reporting_raw.account_number = sbv.account_number
from V068_ad_hoc_box_reporting_raw
inner join vespa_analysts.vespa_single_box_view as sbv
on V068_ad_hoc_box_reporting_raw.subscriber_id = sbv.subscriber_id;

commit;
create index for_joins on V068_ad_hoc_box_reporting_raw (account_number);
commit;
go

update V068_ad_hoc_box_reporting_raw
set enable_date = convert(date, '20' || substring(enablement_date,8,2)
    || case upper(substring(V068_ad_hoc_box_reporting_raw.enablement_date,4,3))
        when 'JAN' then '-01-'
        when 'FEB' then '-02-'
        when 'MAR' then '-03-'
        when 'APR' then '-04-'
        when 'MAY' then '-05-'
        when 'JUN' then '-06-'
        when 'JUL' then '-07-'
        when 'AUG' then '-08-'
        when 'SEP' then '-09-'
        when 'OCT' then '-10-'
        when 'NOV' then '-11-'
        when 'DEC' then '-12-'
        else '-FAIL-' end
    || substring(enablement_date,1,2))
where enablement_date is not null;

commit;
go

-- Reporting quality we'll do later maybe

/************ PART A03 : IMPORT QA - WHAT DO WE HAVE TO WORK WITH? ***********/

select count(1)
from V068_ad_hoc_box_reporting_raw;
-- 2005114

select count(1)
from V068_ad_hoc_box_reporting_raw
where account_number is null;
-- 794 - fine, not too many

select count(1)
from V068_ad_hoc_box_reporting_raw
where enable_date is null;
-- 2... that's okay.

select * from V068_ad_hoc_box_reporting_raw
where enable_date is null;
-- One of these is fully reporting, the other never is, so later we'll manually hack
-- box 14142164 to have good reporting quality and otherwise leave the bad reporter
-- alone.

select max(enable_date)
from V068_ad_hoc_box_reporting_raw;
-- Okay, we've got guys that were only just enabled on the 12th of July, which is quite
-- a bit after the dialback period we've got in the data - 4th to 10th of July.

select enable_date, count(1) as hits
from  V068_ad_hoc_box_reporting_raw
group by enable_date
order by enable_date;
-- only 95 boxes with enablement dates >= 2012-07-04, maybe we just
-- discard all of those?

select * from V068_ad_hoc_box_reporting_raw
where enable_date >= '2012-07-04';
-- well, 95 isn't a lot, we'll just exclude them, looks like we'd only lose maybe 10 or
-- so boxes which are regularly reporting (not even considering account aggregation).

/************ PART B01 : REPORTING QUALITY METRIC BY BOX ***********/

-- This guy is kind of messy because we have alternate panels in there too. So
-- we're going to give Vespa panel +1 for each day it returns data, and the
-- alternate panels get +1 for each correct day of data return and -0.3 for
-- each instance of data return on an incorrect day. Then we get to normalise
-- these numbers against how many data returns we expected, which is still
-- not very many; only like 3 for ALT6 panel. Not very good visibility, but
-- whatever.

update V068_ad_hoc_box_reporting_raw
set reporting_score = 
     case dr_040712 when 'Y' then 1 else 0 end * case when panel in (12,7) then 1 else 0.2 end
    +case dr_050712 when 'Y' then 1 else 0 end * case when panel in (12,6) then 1 else 0.2 end
    +case dr_060712 when 'Y' then 1 else 0 end * case when panel in (12,7) then 1 else 0.2 end
    +case dr_070712 when 'Y' then 1 else 0 end * case when panel in (12,6) then 1 else 0.2 end
    +case dr_080712 when 'Y' then 1 else 0 end * case when panel in (12,7) then 1 else 0.2 end
    +case dr_090712 when 'Y' then 1 else 0 end * case when panel in (12,6) then 1 else 0.2 end
    +case dr_100712 when 'Y' then 1 else 0 end * case when panel in (12,7) then 1 else 0.2 end
    +case dr_110712 when 'Y' then 1 else 0 end * case when panel in (12,6) then 1 else 0.2 end
    +case dr_120712 when 'Y' then 1 else 0 end * case when panel in (12,7) then 1 else 0.2 end
    +case dr_130712 when 'Y' then 1 else 0 end * case when panel in (12,6) then 1 else 0.2 end
    +case dr_140712 when 'Y' then 1 else 0 end * case when panel in (12,7) then 1 else 0.2 end
    +case dr_150712 when 'Y' then 1 else 0 end * case when panel in (12,6) then 1 else 0.2 end
    +case dr_160712 when 'Y' then 1 else 0 end * case when panel in (12,7) then 1 else 0.2 end
    +case dr_170712 when 'Y' then 1 else 0 end * case when panel in (12,6) then 1 else 0.2 end
    +case dr_180712 when 'Y' then 1 else 0 end * case when panel in (12,7) then 1 else 0.2 end
    +case dr_190712 when 'Y' then 1 else 0 end * case when panel in (12,6) then 1 else 0.2 end
    +case dr_200712 when 'Y' then 1 else 0 end * case when panel in (12,7) then 1 else 0.2 end
    +case dr_210712 when 'Y' then 1 else 0 end * case when panel in (12,6) then 1 else 0.2 end
    +case dr_220712 when 'Y' then 1 else 0 end * case when panel in (12,7) then 1 else 0.2 end
    +case dr_230712 when 'Y' then 1 else 0 end * case when panel in (12,6) then 1 else 0.2 end
    +case dr_240712 when 'Y' then 1 else 0 end * case when panel in (12,7) then 1 else 0.2 end
;

commit;
go

-- From the looks of it, we've still got a bunch of boxes that are returning data every day
-- for the alternate panels. Whatever then. 
    
-- Now to convert these to a kind of reporting metric; of course, the normalisation depends
-- on which panel they were on, because there are different numbers of days on which we'd
-- expect dialbacks.
update V068_ad_hoc_box_reporting_raw
set reporting_quality = round(reporting_score / case panel
        when 12 then 21     -- panel 12 has 21 days they should be reporting
        when 6  then 10     -- panel 6 should only be reporting for 10 days
        when 7  then 11     -- panel 7 should report for 11 days in this period
    end, 4)
;

commit;
go

-- These numbers exceed 1 for 6 and 7 because some boxes report on all the days they should
-- and then also on some days they shouldn't.

select count(1) from V068_ad_hoc_box_reporting_raw
where reporting_quality = 0;
-- 585,657 - so we can instantly boot half a million boxes off the panel
-- (though we kind of want to get the account view first)

select top 60 reporting_quality, count(1) as hits
from V068_ad_hoc_box_reporting_raw
group by reporting_quality
order by reporting_quality desc;

-- So we've got about 1.2m boxes with reporting quality 0.8 or above. Wonder
-- how this translates to accounts, because that'll be the important one.

/************ PART B01 : GROUPING INTO ACCOUNT LEVEL ***********/

-- So now we have reporting quality by box, we need to convert that to
-- account level. We're going to apply a rough hack or what we do in the
-- panel management report, but with slightly different limits as we've
-- got a different range of data on box dialback.

-- This form roughly mirrors the PanMan build, though we still need to
-- check that we have enough boxes reporting for the accounts we expect?
-- hmmm, mayb e not, because we do have big listings of boxes that have
-- not returned data over the period of interest.

select
    account_number
    ,min(panel)                     as panel              -- This guy should be unique per account, we test for that coming off SBV
    ,count(1)                       as hh_box_count
    ,max(enable_date)               as recent_enablement
    ,min(reporting_quality)         as min_reporting_quality  -- Used much later in the box selection bit, but may as well build it now
    ,max(reporting_quality)         as max_reporting_quality
    ,case
        when min_reporting_quality >= 0.8   then 'Acceptable'
        when max_reporting_quality = 0      then 'Zero reporting'
                                            else 'Unreliable'
      end                           as reporting_categorisation    
into V068_data_return_by_account
from V068_ad_hoc_box_reporting_raw
group by account_number;

commit;
create unique index fake_pk on V068_data_return_by_account (account_number);
commit;
go

select panel, reporting_categorisation, count(1) as accounts
from V068_data_return_by_account
group by panel, reporting_categorisation
order by panel, reporting_categorisation;
/*
12,'Acceptable    ',435573
12,'Unreliable    ',257287
12,'Zero reporting',168813
7,'Acceptable    ',231508
7,'Unreliable    ',50480
7,'Zero reporting',87795
6,'Acceptable    ',237804
6,'Unreliable    ',43422
6,'Zero reporting',88717
*/

/************ COMPARISON TO EXPECTED NUMBER OF BOXES ***********/

-- Wait, are there instances where we have multiroom stuff but the PanMan segmentation
-- we inheirated thought there should be more boxes?
select drba.reporting_categorisation, count(1)
from V068_data_return_by_account as drba
inner join V068_sky_base_profiling as sbp
on drba.account_number = sbp.account_number
where drba.hh_box_count < mr_boxes + 1 -- +1 because mr_boxes doesn't count the primary
group by drba.reporting_categorisation;
/* Exactly 6000 in total
'Zero reporting',1770
'Acceptable    ',2709
'Unreliable    ',1521
*/

-- Yeah, mark those 'Acceptable' ones as 'Unreliable' because there's a box missing
update V068_data_return_by_account
set reporting_categorisation = 'Unreliable'
from V068_data_return_by_account
inner join V068_sky_base_profiling as sbp
on V068_data_return_by_account.account_number = sbp.account_number
where V068_data_return_by_account.hh_box_count < mr_boxes + 1
and reporting_categorisation = 'Acceptable';
-- 2709 updated, good.
commit;
go

grant select on V068_data_return_by_account to greenj, dbarnett, jacksons, stafforr, sarahm, gillh, rombaoad, louredaj, patelj, kinnairt;
commit;
go

/************ SEGMENTATION AND ASSESSMENT ***********/

-- Here is where we mix in the Scaling 2 variables, which we saved from a
-- demo build of PanMan a while ago (like, the build of 2012-07-19, which
-- is only like a week away from the box return data we have). All that
-- happens in a different file though, this one just handles the import
-- (okay and a bit of preprocessing to account level).
