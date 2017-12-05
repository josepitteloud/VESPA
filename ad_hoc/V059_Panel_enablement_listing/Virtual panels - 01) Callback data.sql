--Vespa viewing card analysis 09/11/2011 JG
--data is in greenj on P10
--Amended with new data Feb 2013 JG

  -- ##### Callback data ######
---Load in individual files for callback data----
if object_id('VirtPan_01_callback_data') is not null then drop table VirtPan_01_callback_data end if;
create table VirtPan_01_callback_data
  (row_id     bigint identity,
   raw_record varchar(35),
   filename   varchar(14)
);


truncate table VirtPan_01_callback_data;

---Need to convert each of the cardid_val from Hex to Decimal and then multiply by column value and the sum of these columns = the cardid
--hex dialling prefix (e.g. use a lookup such as that done in excel example (this list likely to have more than featured there)

create variable @dt int; --day
create variable @mn int; --month
create variable @yr int; --year
create variable @fl int; --file 1 to 4
create variable @var_file varchar(15000);
--select filename,count(1) from VirtPan_01_callback_data group by filename

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
                            set @var_file='load table VirtPan_01_callback_data(raw_record) from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/CLBK/CLBK' || @fl || '_' || right('0' || @mn, 2) || right('0' || @dt, 2) || @yr || '.dat_prefix'' QUOTES OFF ESCAPES OFF delimited by ''\n'' '
                        end
                else
                        begin
                            set @var_file='load table VirtPan_01_callback_data(raw_record) from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/CLBK/CLBK' || @fl || '_' || right('0' || @mn, 2) || right('0' || @dt, 2) || @yr || '_prefix'' QUOTES OFF ESCAPES OFF delimited by ''\n'' '
                        end
                execute(@var_file)
                update VirtPan_01_callback_data set filename = 'CLBK' || @fl || '_' || right('0' || @mn, 2) || right('0' || @dt, 2) || @yr  where filename is null
                set @dt = @dt + 1
                commit
            end
            set @mn = @mn + 1
        end
        set @yr = @yr + 1
    end
    set @fl = @fl + 1
end;
-- drop table VirtPan_01_callback_data_bak;
-- select * into VirtPan_01_callback_data_bak from VirtPan_01_callback_data
-- drop table VirtPan_01_callback_data;
-- select * into VirtPan_01_callback_data from VirtPan_01_callback_data_bak;

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
                set @var_file='load table VirtPan_01_callback_data(raw_record) from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/CLBK/CLBK' ||        '_' || right('0' || @mn, 2) || right('0' || @dt, 2) || @yr || '_prefix''     QUOTES OFF ESCAPES OFF delimited by ''\n'' '
                execute(@var_file)
                update VirtPan_01_callback_data set filename = 'CLBK' || '_' || right('0' || @mn, 2) || right('0' || @dt, 2) || @yr  where filename is null
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
                     set @var_file='load table VirtPan_01_callback_data(raw_record) from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/CLBK/CLBK' || @fl || '_' || right('0' || @mn, 2) || right('0' || @dt, 2) || '2012.dat_prefix''     QUOTES OFF ESCAPES OFF delimited by ''\n'' '
                end
                else
                begin
                     set @var_file='load table VirtPan_01_callback_data(raw_record) from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/CLBK/CLBK' || @fl || '_' || right('0' || @mn, 2) || right('0' || @dt, 2) || '2012_prefix''     QUOTES OFF ESCAPES OFF delimited by ''\n'' '
                end
                execute(@var_file)
                update VirtPan_01_callback_data set filename = 'CLBK' || @fl || '_' || right('0' || @mn, 2) || right('0' || @dt, 2) || '2012'  where filename is null
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
            set @var_file='load table VirtPan_01_callback_data(raw_record) from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/CLBK/CLBK' || @fl || '_' || right('0' || @mn, 2) || right('0' || @dt, 2) || @yr || '_prefix''     QUOTES OFF ESCAPES OFF delimited by ''\n'' '
            execute(@var_file)
            update VirtPan_01_callback_data set filename = 'CLBK' || @fl || '_' || right('0' || @mn, 2) || right('0' || @dt, 2) || @yr  where filename is null
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
create table manual_load_files(filenamee varchar(50)
                              ,id       int identity);

load table manual_load_files(filenamee) from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/CLBK/manual_load_file.csv' QUOTES OFF ESCAPES OFF delimited by '\n';

set @fl = (select min(id) from manual_load_files)
while @fl <= (select max(id) from manual_load_files)
begin
    set @var_file = (select filenamee from manual_load_files where id = @fl)
    execute('load table VirtPan_01_callback_data(raw_record) from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/CLBK/' || left(@var_file, len(@var_file)-1) || ''' QUOTES OFF ESCAPES OFF delimited by ''\n'' ')
    set @fl = @fl + 1
end
;


/*
  -- Not all the files above are available so copying what JG has initially
  -- loaded and subsequently reprocessing the remaining part
insert into VirtPan_01_callback_data(raw_record, filename)
  select raw_record, filenam
    from greenj.callback_data
   where length(raw_record) = 33;     -- ~50 junk records
commit;
*/


/*
  FORMAT:
  ssssssNNNNNNNNccccccccTpppppppppp

      ssssss     = STBid
      NNNNNNNN   = hex of the STB serial number
      cccccccc   = hex of the Viewing Card number (the decimal CardId is without check-digit)
      T          = type of callback (1,2,3,4,5)
      pppppppppp = hex-ASCI                            I of the dialling prefix
*/

alter table VirtPan_01_callback_data add hex_stb_id          varchar(6);
alter table VirtPan_01_callback_data add hex_stb_serial_no   varchar(8);
alter table VirtPan_01_callback_data add hex_viewing_card_id varchar(8);
alter table VirtPan_01_callback_data add callback_type       varchar(1);
alter table VirtPan_01_callback_data add hexascii_prefix     varchar(10);
alter table VirtPan_01_callback_data add prefix              varchar(5);

update VirtPan_01_callback_data
   set hex_stb_id                  = substr(raw_record, 1, 6)
      ,hex_stb_serial_no           = substr(raw_record, 7, 8)
      ,hex_viewing_card_id         = substr(raw_record,15, 8)
      ,callback_type               = substr(raw_record,23, 1)
      ,hexascii_prefix             = substr(raw_record,24,10)
commit;

update VirtPan_01_callback_data
   set prefix = case
                  when length(hexascii_prefix) <> 10 then 'XXX'
                    else
                        case
                          when substr(hexascii_prefix,1,2) between '30' and '39' then substr(hexascii_prefix, 2,1)
                          when substr(hexascii_prefix,1,2) = '00' then ''
                          when substr(hexascii_prefix,1,2) = '20' then ''
                            else '?'
                        end ||
                        case
                          when substr(hexascii_prefix,3,2) between '30' and '39' then substr(hexascii_prefix, 4,1)
                          when substr(hexascii_prefix,3,2) = '00' then ''
                          when substr(hexascii_prefix,3,2) = '20' then ''
                            else '?'
                        end ||
                        case
                          when substr(hexascii_prefix,5,2) between '30' and '39' then substr(hexascii_prefix, 6,1)
                          when substr(hexascii_prefix,5,2) = '00' then ''
                          when substr(hexascii_prefix,5,2) = '20' then ''
                            else '?'
                        end ||
                        case
                          when substr(hexascii_prefix,7,2) between '30' and '39' then substr(hexascii_prefix, 8,1)
                          when substr(hexascii_prefix,7,2) = '00' then ''
                          when substr(hexascii_prefix,7,2) = '20' then ''
                            else '?'
                        end ||
                        case
                          when substr(hexascii_prefix,9,2) between '30' and '39' then substr(hexascii_prefix, 10,1)
                          when substr(hexascii_prefix,9,2) = '00' then ''
                          when substr(hexascii_prefix,9,2) = '20' then ''
                            else '?'
                        end
                end;
commit;


--convert serial no. and card id from hex
alter table VirtPan_01_callback_data
  add (stb_serial_no    bigint      default null,
       cardid           bigint      default null,
       nds_stb_no       varchar(30) default null,
       dt               date        default null,
       account_number   varchar(30) default null,
       subscriber_id    int         default null,
       callback_seq     smallint    default null);

create hg index idx_cardid_hg on VirtPan_01_callback_data(cardid);
create hg index idx_subscriber_id_hg on VirtPan_01_callback_data(subscriber_id);
create hg index idx_nds_stb_no_hg on VirtPan_01_callback_data(nds_stb_no);
create hg index idx1 on VirtPan_01_callback_data(account_number);


update VirtPan_01_callback_data
   set stb_serial_no = hextoint(hex_stb_serial_no),
       cardid        = hextoint(hex_viewing_card_id),
       nds_stb_no    = hex_stb_id || right('0000000000' || stb_serial_no, 10),
       dt            = right(filename,4) || '-' || substr(filename,len(filename)-7,2) || '-' || substr(filename, len(filename)-5,2);
commit;


update VirtPan_01_callback_data as bas
   set subscriber_id = cast(si_external_identifier as int)
  from sk_prod.cust_service_instance as csi
 where bas.nds_stb_no = csi.decoder_nds_number;
commit;


  -- ##### Get account-sub Id link #####
if object_id('VirtPan_tmp_account_subid_lookup') is not null then drop table VirtPan_tmp_account_subid_lookup end if;
select
      account_number,
      cast(card_subscriber_id as int) as subscriber_id
  into VirtPan_tmp_account_subid_lookup
  from sk_prod.cust_card_subscriber_link
 where current_flag = 'Y'
 group by account_number, card_subscriber_id, service_instance_id;
commit;

create hg index idx1 on VirtPan_tmp_account_subid_lookup(account_number);


  -- Get Subscriber Ids allocated to multiple Account Numbers
if object_id('VirtPan_tmp_Ambiguous_Sub_Ids') is not null then drop table VirtPan_tmp_Ambiguous_Sub_Ids end if;
select
      subscriber_id,
      count(*) as cnt
  into VirtPan_tmp_Ambiguous_Sub_Ids
  from VirtPan_tmp_account_subid_lookup
 group by subscriber_id having count(*) > 1;
commit;
create unique hg index idx2 on VirtPan_tmp_Ambiguous_Sub_Ids(subscriber_id);

  -- Delete these accounts
delete from VirtPan_tmp_account_subid_lookup
 where account_number in (select
                                account_number
                            from VirtPan_tmp_Ambiguous_Sub_Ids a,
                                 VirtPan_tmp_account_subid_lookup b
                           where a.subscriber_id = b.subscriber_id);
commit;

create unique hg index idx2 on VirtPan_tmp_account_subid_lookup(subscriber_id);

  -- Append Account Number info
update VirtPan_01_callback_data as bas
   set account_number = cid.account_number
  from VirtPan_tmp_account_subid_lookup as cid
 where bas.subscriber_id = cid.subscriber_id;
commit;

  -- ##### Create callback sequence #####
if object_id('VirtPan_tmp_callback_seq') is not null then drop table VirtPan_tmp_callback_seq end if;
select
      row_id,
      rank () over (partition by subscriber_id order by dt desc, prefix desc, row_id desc) callback_seq
  into VirtPan_tmp_callback_seq
  from VirtPan_01_callback_data;
commit;

create unique hg index idx2 on VirtPan_tmp_callback_seq(row_id);

update VirtPan_01_callback_data base
   set base.callback_seq  = det.callback_seq
  from VirtPan_tmp_callback_seq det
 where base.row_id = det.row_id;
commit;




  -- ###### SCMS callback ######
if object_id('VirtPan_02_SCMS_callback_data') is not null then drop table VirtPan_02_SCMS_callback_data end if;
CREATE TABLE "VirtPan_02_SCMS_callback_data" (
    id                 int         identity,
    subscriber_ID      int         DEFAULT NULL,
    STB_Make_Model     varchar(50) DEFAULT NULL,
    Cbk_Day            smallint    DEFAULT NULL,
    Missing_Cbcks      smallint    DEFAULT null,
    gt12_Hours_Late    smallint    DEFAULT null,
    gt5_Minutes_Late   smallint    DEFAULT null,
    x8_Hour_Attempt    smallint    DEFAULT null,
    x4_Hour_Attempt    smallint    DEFAULT null,
    On_Time            smallint    DEFAULT null,
    Expected_Cbcks     smallint    DEFAULT null,
    account_number     varchar(50) default null
);


truncate table VirtPan_02_SCMS_callback_data;
commit;
load table VirtPan_02_SCMS_callback_data
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


delete from VirtPan_02_SCMS_callback_data
 where subscriber_id in (select subscriber_id from VirtPan_02_SCMS_callback_data group by subscriber_id having count(*) > 1);
commit;

create unique hg index idx_subscriber_id_hg on VirtPan_02_SCMS_callback_data(subscriber_id);
create hg index idx_account_number_hg on VirtPan_02_SCMS_callback_data(account_number);


update VirtPan_02_SCMS_callback_data as bas
   set bas.account_number = csi.account_number
  from VirtPan_tmp_account_subid_lookup csi
 where bas.subscriber_id = csi.subscriber_id;
commit;
















