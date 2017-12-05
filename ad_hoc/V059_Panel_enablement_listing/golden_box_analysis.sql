--Vespa viewing card analysis 09/11/2011 JG
--data is in greenj on P10
--Amended with new data Feb 2013 JG

---Load in individual files for callback data----
drop table callback_data;
create table callback_data
(raw_record varchar(20000)
,filenam varchar(14)
)
;

select count(1) from callback_data;
truncate table callback_data;

---Need to convert each of the cardid_val from Hex to Decimal and then multiply by column value and the sum of these columns = the cardid
--hex dialling prefix (e.g. use a lookup such as that done in excel example (this list likely to have more than featured there)

create variable @dt int; --day
create variable @mn int; --month
create variable @yr int; --year
create variable @fl int; --file 1 to 4
create variable @var_file varchar(1000);
--select filenam,count(1) from callback_data group by filenam

--loop to import the data 2007-20120411
set @fl=1;
while @fl <= 4
begin
    set @yr = 2007
    while @yr <= 2012
    begin
        set @mn = 1
        while @mn <= 12
        begin
            set @dt = 1
            while (@dt <=28 or (@dt in (29,30) and @mn <> 2) or (@dt = 31 and @mn in (1,3,5,7,8,10,12)) or (@dt = 29 and @mn = 2 and @yr in (2008,2012)))  and @yr || right('0' || @mn, 2) || right('0' || @dt, 2) between '20070918' and '20120411'
            begin
                if @yr >= 2011 or (@fl = 3 and @yr = 2007 and @mn >= 10) or (@fl = 3 and @yr >= 2008)
                        begin
                            set @var_file='load table callback_data(raw_record) from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/CLBK/CLBK' || @fl || '_' || right('0' || @mn, 2) || right('0' || @dt, 2) || @yr || '.dat_prefix'' QUOTES OFF ESCAPES OFF delimited by ''\n'' '
                        end
                else
                        begin
                            set @var_file='load table callback_data(raw_record) from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/CLBK/CLBK' || @fl || '_' || right('0' || @mn, 2) || right('0' || @dt, 2) || @yr || '_prefix'' QUOTES OFF ESCAPES OFF delimited by ''\n'' '
                        end
                execute(@var_file)
                update callback_data set filenam = 'CLBK' || @fl || '_' || right('0' || @mn, 2) || right('0' || @dt, 2) || @yr  where filenam is null
                set @dt = @dt + 1
                commit
            end
            set @mn = @mn + 1
        end
        set @yr = @yr + 1
    end
    set @fl = @fl + 1
end;
drop table callback_data_bak;
select * into callback_data_bak from callback_data
drop table callback_data;
select * into callback_data from callback_data_bak;

--pre 2004
set @yr = 1999;
while @yr <= 2005
begin
    set @mn = 1
    while @mn <= 12
    begin
        if (@mn in (1,7,8,9,10) and @yr  >= 2000) or (@mn in (2,3,4,5,6) and @yr >= 2001) or (@mn in (11,12) and @yr <= 2004)
        begin
            set @dt = 1
            while @dt <=28 or (@dt in (29,30) and @mn <> 2) or (@dt = 31 and @mn in (1,3,5,7,8,10,12)) or (@dt = 29 and @mn = 2 and @yr = 2004)
            begin
                set @var_file='load table callback_data(raw_record) from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/CLBK/CLBK' ||        '_' || right('0' || @mn, 2) || right('0' || @dt, 2) || @yr || '_prefix''     QUOTES OFF ESCAPES OFF delimited by ''\n'' '
                execute(@var_file)
                update callback_data set filenam = 'CLBK' || '_' || right('0' || @mn, 2) || right('0' || @dt, 2) || @yr  where filenam is null
               set @dt = @dt + 1
               commit
            end
        end
        set @mn = @mn + 1
    end
    set @yr = @yr + 1
end
;

--20120412-20120630
set @fl = 1;
while @fl <=4
begin
    set @mn = 4
    while @mn <= 6
    begin
        set @dt = 1
        if (@mn = 4)
        begin
             set @dt=12
        end
        while @dt <=30 or (@dt = 31 and @mn = 5)
        begin
            if ((@dt <= 2) or (@mn < 7))
            begin
                if (@fl = 1 or @fl = 3)
                begin
                     set @var_file='load table callback_data(raw_record) from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/CLBK/CLBK' || @fl || '_' || right('0' || @mn, 2) || right('0' || @dt, 2) || '2012.dat_prefix''     QUOTES OFF ESCAPES OFF delimited by ''\n'' '
                end
                else
                begin
                     set @var_file='load table callback_data(raw_record) from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/CLBK/CLBK' || @fl || '_' || right('0' || @mn, 2) || right('0' || @dt, 2) || '2012_prefix''     QUOTES OFF ESCAPES OFF delimited by ''\n'' '
                end
                execute(@var_file)
                update callback_data set filenam = 'CLBK' || @fl || '_' || right('0' || @mn, 2) || right('0' || @dt, 2) || '2012'  where filenam is null
                commit
            end
            set @dt = @dt + 1
        end
        set @mn = @mn + 1
    end
    set @fl = @fl + 1
end
;

--Jul 2012 - Feb 2013
set @fl = 1;
while @fl <=4
begin
    set @yr=2012
    set @mn = 7
    while @mn <> 3
    begin
        set @dt = 1
        while @dt <=9 or (@dt <= 30 and @mn <> 2) or (@dt = 31 and @mn in (7, 8, 10, 12, 1))
        begin
            set @var_file='load table callback_data(raw_record) from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/CLBK/CLBK' || @fl || '_' || right('0' || @mn, 2) || right('0' || @dt, 2) || @yr || '_prefix''     QUOTES OFF ESCAPES OFF delimited by ''\n'' '
            execute(@var_file)
            update callback_data set filenam = 'CLBK' || @fl || '_' || right('0' || @mn, 2) || right('0' || @dt, 2) || @yr  where filenam is null
            commit
            set @dt = @dt + 1
        end
        set @mn = @mn + 1
        if (@mn = 13)
        begin
            set @mn = 1
            set @yr = @yr + 1
        end
    end
    set @fl = @fl + 1
end
;

--process supplemental files manually
create table manual_load_files(filename varchar(50)
                              ,id       int identity);

load table manual_load_files(filename) from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/CLBK/manual_load_file.csv' QUOTES OFF ESCAPES OFF delimited by '\n';

set @fl = (select min(id) from manual_load_files)
while @fl <= (select max(id) from manual_load_files)
begin
    set @var_file = (select filename from manual_load_files where id = @fl)
    execute('load table callback_data(raw_record) from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/CLBK/' || left(@var_file, len(@var_file)-1) || ''' QUOTES OFF ESCAPES OFF delimited by ''\n'' ')
    set @fl = @fl + 1
end
;

alter table callback_data add hex_stb_id          varchar(6);
alter table callback_data add hex_stb_serial_no   varchar(8);
alter table callback_data add hex_viewing_card_id varchar(8);
alter table callback_data add callback_type       varchar(1);
alter table callback_data add hexascii_prefix     varchar(10);
alter table callback_data add prefix     varchar(5);

update callback_data
   set hex_stb_id                  = substr(raw_record, 1, 6)
      ,hex_stb_serial_no           = substr(raw_record, 7, 8)
      ,hex_viewing_card_id         = substr(raw_record,15, 8)
      ,callback_type               = substr(raw_record,23, 1)
      ,hexascii_prefix             = substr(raw_record,24,10)

  update callback_data
     set prefix = case when substr(hexascii_prefix,1,2) between '30' and '39' then substr(hexascii_prefix, 2,1) when substr(hexascii_prefix,1,2) = '20' then ' ' else '?' end ||
                  case when substr(hexascii_prefix,3,2) between '30' and '39' then substr(hexascii_prefix, 4,1) when substr(hexascii_prefix,3,2) = '20' then ' ' else '?' end ||
                  case when substr(hexascii_prefix,5,2) between '30' and '39' then substr(hexascii_prefix, 6,1) when substr(hexascii_prefix,5,2) = '20' then ' ' else '?' end ||
                  case when substr(hexascii_prefix,7,2) between '30' and '39' then substr(hexascii_prefix, 8,1) when substr(hexascii_prefix,7,2) = '20' then ' ' else '?' end ||
                  case when substr(hexascii_prefix,9,2) between '30' and '39' then substr(hexascii_prefix,10,1) when substr(hexascii_prefix,9,2) = '20' then ' ' else '?' end

--convert serial no. and card id from hex
alter table callback_data add stb_serial_no bigint;
alter table callback_data add cardid bigint;
  update callback_data
     set stb_serial_no = hextoint(hex_stb_serial_no)
    , cardid           = hextoint(hex_viewing_card_id)
;
commit;

alter table callback_data add nds_stb_no varchar(30);
update callback_data set nds_stb_no = hex_stb_id || right('0000000000' || stb_serial_no, 10);

--add date to calculate latest info
   alter table callback_data add dt date;
  update callback_data set dt=right(filenam,4) || '-' || substr(filenam,len(filenam)-7,2) || '-' || substr(filenam,len(filenam)-5,2)

--add account number
alter table callback_data add account_number varchar(30);
alter table callback_data add subscriber_id int;

create hg index idx_cardid_hg on callback_data(cardid);
create hg index idx_subscriber_id_hg on callback_data(subscriber_id);
create hg index idx_nds_stb_no_hg on callback_data(nds_stb_no);

  update callback_data as bas
     set account_number = cid.account_number
    from sk_prod.cust_card_issue_dim as cid
   where cast(bas.cardid as varchar) = left(cid.card_id,8)
; --

  update callback_data as bas
     set subscriber_id = cast(si_external_identifier as int)
    from sk_prod.cust_service_instance as csi
   where bas.nds_stb_no = csi.decoder_nds_number
; --

  update callback_data as bas
     set bas.account_number = sbv.account_number
    from Vespa_Analysts.Vespa_Single_Box_View as sbv
   where bas.subscriber_id = sbv.subscriber_id
     and bas.account_number is null
;


drop table golden_boxes;
CREATE TABLE "golden_boxes" (
    id                 int         identity,
    subscriber_ID      varchar(8)  DEFAULT NULL,
    STB_Make_Model     varchar(50) DEFAULT NULL,
    Cbk_Day            smallint    DEFAULT NULL,
    Missing_Cbcks      smallint    DEFAULT null,
    gt12_Hours_Late    smallint    DEFAULT null,
    gt5_Minutes_Late   smallint    DEFAULT null,
    x8_Hour_Attempt    smallint    DEFAULT null,
    x4_Hour_Attempt    smallint    DEFAULT null,
    On_Time            smallint    DEFAULT null,
    Expected_Cbcks     smallint    DEFAULT null,

    model_number       varchar(20) default null,
    anytimeplus        bit         default 0,
    nds_stb_no         varchar(50) default null,
    src_system_id      varchar(50) default null,
    account_number     varchar(50) default null,
    prefix             varchar(10) default null,
    latest_Callback    date        default '9999-09-09',
    box_type           varchar(30) default null
);


truncate table golden_boxes;
commit;
load table golden_boxes
(
  subscriber_ID',',
  STB_Make_Model',',
  Cbk_Day',',
  Missing_Cbcks',',
  gt12_Hours_Late',',
  gt5_Minutes_Late',',
  x8_Hour_Attempt',',
  x4_Hour_Attempt',',
  On_Time',',
  Expected_Cbcks'\n'
)
from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/SCMS_callback_analysis_Jan-2013.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 200000
DELIMITED BY ','
SKIP 1
;

  select account_number
        ,si_external_identifier as subscriber_id
    into #csi
    from sk_prod.cust_service_instance
   where subscriber_id <> '0' --to eliminate never-ending queries
group by account_number
        ,subscriber_id
; --2m

create hg index idx_subscriber_id_hg on #csi(subscriber_id);
create hg index idx_subscriber_id_hg on golden_boxes(subscriber_id);

  update golden_boxes as bas
     set bas.account_number = #csi.account_number
    from #csi
   where bas.subscriber_id = #csi.subscriber_id
;

create hg index idx_account_number_hg on golden_boxes(account_number);

  update golden_boxes as bas
     set anytimeplus = 1
    from sk_prod.cust_subs_hist as csh
   where bas.account_number = csh.account_number
     and subscription_sub_type='PDL subscriptions'  --anytime plus subscription
     and status_code='AC'
     and first_activation_dt<'9999-09-09'         -- (END)
     and first_activation_dt>='2010-10-01'        -- (START) Oct 2010 was the soft launch of A+, no one should have it before then
;--4795330

  update golden_boxes as gol
     set gol.nds_stb_no = cal.nds_stb_no
    from callback_data as cal
   where cast(gol.subscriber_id as int)= cal.subscriber_id
; --8,213,885


  update golden_boxes as gol
     set src_system_id = csi.src_system_id
    from sk_prod.cust_service_instance as csi
   where gol.subscriber_id = csi.si_external_identifier
     and gol.subscriber_id <> '0'
; --6,556,221

create hg index idx_src_system_id_hg on golden_boxes(src_system_id);

select * into #stb_active from
     (select service_instance_id
            ,decoder_nds_number
            ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) rank
        from sk_prod.cust_Set_top_box) as sub
 where rank = 1
;--23,940,569

  update golden_boxes as gol
     set gol.nds_stb_no = stb.decoder_nds_number
    from #stb_active as stb
   where gol.src_system_id = stb.service_instance_id
     and gol.src_system_id is not null
; --13,051,315

--find the most recent prefix
  select nds_stb_no
        ,max(dt) as latest_date
        ,null as prefix
    into #latest_prefix
    from callback_data
group by nds_stb_no
; --13,941,562

commit;
create hg index idx1 on #latest_prefix(latest_date,nds_stb_no);
create hg index idx2 on callback_data(dt,nds_stb_no);

  update #latest_prefix as lat
     set lat.prefix = cal.prefix
    from callback_data as cal
   where cal.dt = lat.latest_date
     and cal.nds_stb_no = lat.nds_stb_no
; --13,440,812

commit;
create hg index idx2 on golden_boxes(nds_stb_no);
create hg index idx2 on #latest_prefix(nds_stb_no);

  update golden_boxes as gol
     set gol.prefix = cast(lat.prefix as varchar)
        ,gol.latest_callback = latest_date
    from #latest_prefix as lat
   where gol.nds_stb_no = lat.nds_stb_no
; --8,046,707
--        delete from golden_boxes where id<=236073274;
commit;


--add model number.
  update golden_boxes as gol
     set model_number = x_model_number
    from sk_prod.cust_set_top_box as stb
   where gol.src_system_id = stb.service_instance_id
;


select top 100 * from golden_boxes order by id
--expected_cbcks and box_type not populated

select max(latest_callback) from golden_boxes where latest_callback < '9999-09-09'
--2013-02-09


select count(distinct account_number) from golden_boxes
where prefix is null
and latest_callback < '9999-09-09'
and latest_callback > '2013-01-09'

select count(distinct account_number) from golden_boxes where latest_callback = '9999-09-09'
select latest_callback,count(1) from golden_boxes group by latest_callback
commit








/*

drop table golden_summary;
CREATE TABLE "golden_summary" (
 "account_number"     varchar(50)  DEFAULT NULL,
 "nds_stb_no"         varchar(50)  DEFAULT NULL,
 "subscriber_id"      varchar(10)  DEFAULT NULL,
 "percentage"         double       DEFAULT NULL,
 "expected_cbcks"     varchar(3)   DEFAULT NULL,
 "prefix"             varchar(10)  DEFAULT NULL,
 "anytimeplus"        bit NOT NULL DEFAULT NULL,
 "cbk_day"            varchar(2)   DEFAULT NULL,
 "on_time"            varchar(2)   DEFAULT NULL
)
;

insert into golden_summary
  select account_number
        ,nds_stb_no
        ,subscriber_id
        ,case when expected_cbcks = '0' then 0 else cast(left(on_time,1) as real)/ cast(expected_cbcks as real) end as on_time_rate
        ,expected_cbcks
        ,prefix
        ,anytimeplus
        ,cbk_day
        ,on_time
    into golden_summary
    from golden_boxes
;
commit;

grant all on golden_boxes to vespa_group_low_security;
grant all on golden_summary to vespa_group_low_security;



select top 100 * from golden_boxes





--------------------------------------------------------------------------------------------------------------------------------------------------------
--
--Additional Qs
--
--------------------------------------------------------------------------------------------------------------------------------------------------------


--Q 23/05/12:can you please tell me how many boxes out of the total of c12.5m installed are:
--
--1. GOLD:                     Prefix free
--2. PLATINUM:            Prefix free and have dialled back successfully in the past 6 months (from the CA data)
--For 2. Can you split this into 3? Dialled back at least 4, or at least 5 or at least 6 times on time from the golden_box table?
drop table boxes;
select distinct si_external_identifier
  into greenj.boxes
  from sk_prod.cust_service_instance as csi
       inner join sk_prod.cust_single_account_view as sav on csi.account_number = sav.account_number
 where CUST_ACTIVE_DTV = 1
   and effective_to_dt = '9999-09-09'
; --11,748,909

alter table boxes add null_prefix bit default 1;

  select subscriber_id
        ,max(dt) as latest_date
        ,null as prefix
    into latest_prefix --by subscriber_id this time, rather than nds no.
    from callback_data
group by subscriber_id
;

  update latest_prefix as lat
     set lat.prefix = cal.prefix
    from callback_data as cal
   where cal.dt = lat.latest_date
     and cal.subscriber_id = lat.subscriber_id
;

  update boxes as box
     set null_prefix = 0
    from latest_prefix as lat
   where box.si_external_identifier = cast(lat.subscriber_id as varchar)
     and prefix >= 0
;

alter table boxes add dialbacks int;

  update boxes as box
     set box.dialbacks = gol.expected_cbcks
   from golden_boxes as gol
  where box.si_external_identifier = gol.subscriber_id
;

  select case when null_prefix = 1 and dialbacks >= 6 then 'Null and 6+'
              when null_prefix = 1 and dialbacks  = 5 then 'Null and 5'
                                              when null_prefix = 1 and dialbacks  = 4 then 'Null and 4'
              when null_prefix = 1 and dialbacks = -1 then 'Null and no match'
              when null_prefix = 1                    then 'Null and < 4'
              when                     dialbacks = -1 then 'not null and no match'
              else 'Others' end as type
        ,count(1) as cow
    from boxes
group by type
;

commit;
create hg index idx1 on #new_subs(subscriber_id);
create hg index idx1 on #old_subs(subscriber_id);

  select distinct(subscriber_id)
    into #new_subs
    from callback_data
   where right(filenam,4) = '2012'
     and (    left(right(filenam,8),2) in ('05','06','07')
          or (left(right(filenam,8),2) = '04' and left(right(filenam,6),2) >= '12'))
;
commit;
create hg index idx1 on #new_subs(subscriber_id);
create hg index idx1 on #old_subs(subscriber_id);

  select distinct(subscriber_id)
    into #new_subs
    from callback_data
   where right(filenam,4) = '2012'
     and (    left(right(filenam,8),2) in ('05','06','07')
          or (left(right(filenam,8),2) = '04' and left(right(filenam,6),2) >= '12'))
;

  select distinct(subscriber_id)
    into #old_subs
    from callback_data
   where right(filenam,4)         < '2012'
      or left(right(filenam,8),2) < '04'
      or left(right(filenam,6),2) < '12'
;

  select distinct (nsb.subscriber_id)
    from #new_subs as nsb
         left join #old_subs as osb on osb.subscriber_id = nsb.subscriber_id
   where osb.subscriber_id is null

execute('
  select distinct (nsb.subscriber_id)
    into ' || user || '.new_new
    from #new_subs as nsb
         left join #old_subs as osb on osb.subscriber_id = nsb.subscriber_id
   where osb.subscriber_id is null
');

--August 2012: file of newly created accounts was added to callback_data.
--A list of just the new subscriber IDs has been created in the table new_new
--so we can create a list of subscribers that pass therough the waterfall and have Anytime+ and null prefix
  select distinct(cal.subscriber_id)
    from new_new                                       as nnw
         inner join callback_data                      as cal on nnw.subscriber_id  = cal.subscriber_id
         inner join PanMan_Golden_account_waterfalling as pan on cal.account_number = pan.account_number
         inner join golden_boxes                       as gol on nnw.subscriber_id  = cast(gol.subscriber_id as int)
   where waterfall_exit is null or waterfall_exit in (24, 25)
     and gol.prefix is null
     and anytimeplus = 1
; -- new subscribers


  select model_number
        ,count(1) as cow
    from golden_boxes
   where prefix is not null
group by model_number
;

select top 10 * from golden_boxes

--Sep 2012
   select gol.account_number
         ,model_number
         ,prefix
         ,prod_ph_subs_account_sub_type
    into #breakdown
    from golden_boxes                               as gol
         left join sk_prod.cust_single_account_view as sav on gol.account_number = sav.account_number

  select model_number
        ,prod_ph_subs_account_sub_type as account_type
        ,prefix
        ,count(1) as account_count
    from #breakdown
group by prod_ph_subs_account_sub_type
        ,model_number
        ,prefix

--V2
  select model_number
        ,prod_ph_subs_account_sub_type as account_type
        ,case when account_type in ('Normal', 'Staff', 'Test/Development') then account_type else 'Other' end as type_summarised
        ,prefix
,last12m
,case when box_type in (
        ,count(1) as account_count
    from #breakdown
group by prod_ph_subs_account_sub_type
        ,model_number
        ,prefix














select top 10 * from callback_data
select dt,count(1) from callback_data group by dt





select top 10 * from sk_prod.vespa_dp_prog_viewed_current

*/

select count(distinct bas.account_number) from callback_data as bas
inner join vespa_analysts.vespa_single_box_view as sbv on bas.account_number = sbv.account_number
where dt between '2013-01-10' and '2013-02-09'

select max(dt) from callback_data



  select count(distinct sbv.account_number)
    from vespa_analysts.vespa_single_box_view as sbv
         inner join #golde as gol on sbv.account_number = gol.account_number
   where status_vespa='Enabled'
     and dt >= @current-180
     and reporting_quality>0



select count(distinct account_number)
    from vespa_analysts.vespa_single_box_view as sbv
where reporting_quality>0


select top 10 account_number
    from vespa_analysts.vespa_single_box_view as sbv
where reporting_quality=1





--comparison of 6 month callbacks. Data from John C
create table #temp(raw_record varchar(500));
execute('load table #temp(raw_record) from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/ca.csv'' QUOTES OFF ESCAPES OFF delimited by ''\n'' ')
select top 10 cast(raw_record as int) from #temp
select count(1) from #temp
--5,248,131

select top 10 * from bednaszs.golden_boxes
select count(1) from bednaszs.golden_boxes where missing_cbcks>0
--8,636,980

select count(1) from bednaszs.golden_boxes as gol
left join vespa_analysts.vespa_single_box_view as sbv on gol.account_number = sbv.account_number
where missing_cbcks<6
and (status_vespa<>'Enabled'
or status_vespa is null)
--5,581,926

select distinct (cast(gol.subscriber_id as int))
into #gol
from bednaszs.golden_boxes as gol
left join vespa_analysts.vespa_single_box_view as sbv on gol.account_number = sbv.account_number
where missing_cbcks<6
and (status_vespa<>'Enabled'
or status_vespa is null)
--5,378,882


select distinct(cast(raw_record as int)) as subid
into #john
from #temp
--5,248,127

select count(1) from #gol
inner join #john on #gol.subscriber_id = #john.subid
--4,673,030

select top 10 * from #john left join #gol on #gol.subscriber_id = #john.subid
where #gol.subscriber_id is null
order by #john.subid desc

select * from bednaszs.golden_boxes
where subscriber_id like '%31643%'--31643573




