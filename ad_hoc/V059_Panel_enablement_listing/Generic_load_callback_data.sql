--Vespa viewing card analysis 09/11/2011 JG
--data is in greenj on P10
--Amended with new data Feb 2013 JG
--March 2013 This Version for use in Vespa Analysts with the Waterfall procedure
--June 2013 amended for running from any schema (that has privileges for vespa_analysts)

  -- ##### Callback data ######
---Load in individual files for callback data----
-- create table Waterfall_callback_data
--   (row_id              bigint identity
--   ,raw_record          varchar(50)
--   ,filename            varchar(14)
--   ,hex_stb_id          varchar(6)
--   ,hex_stb_serial_no   varchar(8)
--   ,hex_viewing_card_id varchar(8)
--   ,callback_type       varchar(1)
--   ,hexascii_prefix     varchar(10)
--   ,prefix              varchar(5)
--   ,stb_serial_no       bigint      default null
--   ,cardid              bigint      default null
--   ,nds_stb_no          varchar(30) default null
--   ,dt                  date        default null
--   ,account_number      varchar(30) default null
--   ,subscriber_id       int         default null
--   ,callback_seq        smallint    default null
-- );
--
-- create hg index idx_cardid_hg        on Waterfall_callback_data(cardid);
-- create hg index idx_subscriber_id_hg on Waterfall_callback_data(subscriber_id);
-- create hg index idx_nds_stb_no_hg    on Waterfall_callback_data(nds_stb_no);
-- create hg index idx1                 on Waterfall_callback_data(account_number);
--

  truncate table vespa_analysts.Waterfall_callback_data;
        -- Need to convert each of the cardid_val from Hex to Decimal and then multiply by column value and the sum of these columns = the cardid
        -- hex dialling prefix (e.g. use a lookup such as that done in excel example (this list likely to have more than featured there)

  create variable @dt int; --day
  create variable @mn int; --month
  create variable @yr int; --year
  create variable @fl int; --file 1 to 4
  create variable @var_file varchar(15000);

      -- loop to import the data 11132013 - 10182014
     set @fl=1;
   while @fl <= 4 begin
               set @yr = 2013
             while @yr <= 2014 begin
                         set @mn = 1
                       while @mn <= 12 begin
                                   set @dt = 1
                                 while @dt <= 31 begin
                                              if (@yr || right('0' || @mn, 2) || right('0' || @dt, 2) between '20131113' and '20141018') and (@dt <= 28 or (@dt in (29, 30) and @mn <> 2) or (@dt = 31 and @mn in (1,3,5,7,8,10,12))) begin
                                                       set @var_file='load table vespa_analysts.Waterfall_callback_data(raw_record) from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/CLBK/current/CLBK' || @fl || '_' || right('0' || @mn, 2) || right('0' || @dt, 2) || @yr || '_prefix'' QUOTES OFF ESCAPES OFF delimited by ''\n'' '
                                                   execute (@var_file)
                                                    update vespa_analysts.Waterfall_callback_data set filename = 'CLBK' || @fl || '_' || right('0' || @mn, 2) || right('0' || @dt, 2) || @yr  where filename is null
                                                    commit
                                             end
                                             set @dt = @dt + 1
                                   end
                                   set @dt = 1
                                   set @mn = @mn + 1
                        end
                        set @mn = 1
                        set @yr = @yr + 1
               end
               set @fl = @fl + 1
     end;



-- --process supplemental files manually
-- create table manual_load_files(filenamee varchar(50)
--                               ,id       int identity)
-- ;
--
-- load table manual_load_files(filenamee) from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/CLBK/manual_load_file.csv' QUOTES OFF ESCAPES OFF delimited by '\n';
--
-- set @fl = (select min(id) from manual_load_files);
-- while @fl <= (select max(id) from manual_load_files)
-- begin
--     set @var_file = (select filenamee from manual_load_files where id = @fl)
--     execute('load table vespa_analysts.Waterfall_callback_data(raw_record) from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/CLBK/' || left(@var_file, len(@var_file)-1) || ''' QUOTES OFF ESCAPES OFF delimited by ''\n'' ')
--     set @fl = @fl + 1
-- end
-- ;
--
--

/*
  FORMAT:
  ssssssNNNNNNNNccccccccTpppppppppp

      ssssss     = STBid
      NNNNNNNN   = hex of the STB serial number
      cccccccc   = hex of the Viewing Card number (the decimal CardId is without check-digit)
      T          = type of callback (1,2,3,4,5)
      pppppppppp = hex-ASCII of the dialling prefix
*/

  update vespa_analysts.Waterfall_callback_data
     set raw_record = replace(raw_record,' ','')
;

  update vespa_analysts.Waterfall_callback_data
     set hex_stb_id                  = substr(raw_record, 1, 6)
        ,hex_stb_serial_no           = substr(raw_record, 7, 8)
        ,hex_viewing_card_id         = substr(raw_record,15, 8)
        ,callback_type               = substr(raw_record,23, 1)
        ,hexascii_prefix             = substr(raw_record,24,10)
  ;
  commit;

  update vespa_analysts.Waterfall_callback_data
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


      -- convert serial no. and card id from hex
  update vespa_analysts.Waterfall_callback_data
     set stb_serial_no = hextoint(hex_stb_serial_no)
        ,cardid        = hextoint(hex_viewing_card_id)
        ,dt            = right(filename,4) || '-' || substr(filename,len(filename)-7,2) || '-' || substr(filename, len(filename)-5,2)
;

  update vespa_analysts.Waterfall_callback_data
     set nds_stb_no    = hex_stb_id || right('0000000000' || stb_serial_no, 10)
;

  commit;

  select distinct(nds_stb_no)
        ,cast (0 as int) as subscriber_id
    into #stbs
    from vespa_analysts.Waterfall_callback_data
; --20,972,280

  update #stbs
     set subscriber_id = cast(coalesce(si_external_identifier,'0') as int)
    from cust_service_instance as csi
   where #stbs.nds_stb_no = csi.decoder_nds_number
     and si_external_identifier <> '?'
; --20,738,113

  commit;
  create hg index idx1 on #stbs(nds_stb_no);

  update vespa_analysts.Waterfall_callback_data as bas
     set subscriber_id = #stbs.subscriber_id
    from #stbs
   where #stbs.nds_stb_no = bas.nds_stb_no
;--45m

  commit;

      -- ##### Get account-sub Id link #####
  select account_number
        ,cast(card_subscriber_id as int) as subscriber_id
    into #account_subid_lookup
    from cust_card_subscriber_link
   where current_flag = 'Y'
group by account_number
        ,card_subscriber_id
        ,service_instance_id
;

  commit;
  create hg index idx1 on #account_subid_lookup(account_number);

  -- Get Subscriber Ids allocated to multiple Account Numbers
  select subscriber_id,
         count(*) as cnt
    into #Ambiguous_Sub_Ids
    from #account_subid_lookup
group by subscriber_id having count(*) > 1
; --2492

  commit;
  create unique hg index idx2 on #Ambiguous_Sub_Ids(subscriber_id);

  -- Delete these accounts
  delete from #account_subid_lookup
   where account_number in (select account_number
                              from #Ambiguous_Sub_Ids a,
                                   #account_subid_lookup b
                             where a.subscriber_id = b.subscriber_id)
;--7769

  commit;
  create unique hg index idx2 on #account_subid_lookup(subscriber_id);

      -- Append Account Number info
  update vespa_analysts.Waterfall_callback_data as bas
     set account_number = cid.account_number
    from #account_subid_lookup as cid
   where bas.subscriber_id = cid.subscriber_id
;--44m

commit;

      -- ##### Create callback sequence #####
  select row_id
        ,rank () over (partition by subscriber_id order by dt desc, prefix desc, row_id) as callback_seq
    into #callback_seq
    from vespa_analysts.Waterfall_callback_data
;

  commit;
  create unique hg index idx2 on #callback_seq(row_id);

  update vespa_analysts.Waterfall_callback_data as bas
     set bas.callback_seq  = det.callback_seq
    from #callback_seq as det
   where bas.row_id = det.row_id
  ;

commit;

      -- ###### SCMS callback ######
    call dba.sp_create_table('vespa_analysts','Waterfall_SCMS_callback_data','
         id                 int         identity
        ,subscriber_ID      int
        ,STB_Make_Model     varchar(50)
        ,date_active        varchar(50)
        ,acc_sbscr          varchar(50)
        ,cbck_sbscr         varchar(50)
        ,Cbk_Day            varchar(50)
        ,Missing_Cbcks      smallint
        ,gt12_Hours_Late    smallint
        ,gt5_Minutes_Late   smallint
        ,x8_Hour_Attempt    smallint
        ,x4_Hour_Attempt    smallint
        ,On_Time            smallint
        ,Expected_Cbcks     smallint
        ,account_number     varchar(50)
    ');
  create unique hg index idx_subscriber_id_hg  on vespa_analysts.Waterfall_SCMS_callback_data(subscriber_id);

  create        hg index idx_account_number_hg on vespa_analysts.Waterfall_SCMS_callback_data(account_number);

truncate table vespa_analysts.Waterfall_SCMS_callback_data;

    load table vespa_analysts.Waterfall_SCMS_callback_data(
         subscriber_ID',',
         date_time_received',',
         STB_Make_Model',',
         gt12_Hours_Late',',
         gt5_Minutes_Late',',
         x8_Hour_Attempt',',
         x4_Hour_Attempt',',
         On_Time',',
         Expected_Cbcks'\n'
)
    from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/CLBK/grb_CBK_analysis_20141110.txt'
  QUOTES  OFF
 ESCAPES OFF
  NOTIFY 200000
DELIMITED BY ','
    SKIP 1
;

  delete from vespa_analysts.Waterfall_SCMS_callback_data
   where subscriber_id in (select subscriber_id
                             from vespa_analysts.Waterfall_SCMS_callback_data
                         group by subscriber_id
                           having count(*) > 1)
;

  commit;

--  update vespa_analysts.Waterfall_SCMS_callback_data
--     set cbk_day = left(cbk_day,2)
;

  update vespa_analysts.Waterfall_SCMS_callback_data as bas
     set bas.account_number = csi.account_number
    from #account_subid_lookup as csi
   where bas.subscriber_id = csi.subscriber_id
;
  commit;

  delete from vespa_analysts.Waterfall_SCMS_callback_data
   where subscriber_id is null
;

  update vespa_analysts.Waterfall_SCMS_callback_data as bas
     set missing_cbcks = expected_cbcks-on_time-x4_hour_attempt-x8_hour_attempt-gt5_minutes_late-gt12_hours_late
;


---






