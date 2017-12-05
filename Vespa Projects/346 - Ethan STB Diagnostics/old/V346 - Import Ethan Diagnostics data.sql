
create or replace variable @sql varchar(10000)
;
create or replace variable @filename_template varchar(100)
;
create or replace variable @filename_date varchar(100)
;
create or replace variable @hour_var int
;
create or replace variable @filename_end varchar(100)
;
create or replace variable @filename_full varchar(100)
;
create or replace variable @process_date date
;
set @process_date ='2015-09-03' -- this is the day the data refers to
;
set @filename_date=cast(@process_date as int)
;
set @filename_template='Ethan_BDC_STB_Parameters_'|| @filename_date
;
set @filename_end='0000.csv'
;

create table #et_technical_raw
(
                id varchar(20)
                ,parameter_name varchar(200)
                ,parameter_value varchar(100)
                ,tstamp_raw varchar(22) -- by doing a varchar 21 we take all the chars except the last Z, char terminator for the timestamp, which is useless to our purposes
)
;

IF OBJECT_ID('et_technical') IS NULL
begin
        create table et_technical
        (
                id varchar(20)
                ,parameter_name varchar(200)
                ,parameter_value varchar(100)
                ,tstamp timestamp
                ,reference_date date
                ,source_filename varchar(100)
        )

                create lf index idx1 on et_technical(id)
                commit
                create lf index idx2 on et_technical(parameter_name)
                commit
                create dttm index idx3 on et_technical(tstamp)
                commit
                create lf index idx4 on et_technical(source_filename)
                commit
                create date index idx5 on et_technical(reference_date)
                commit
end
;

set @hour_var=0
;

while @hour_var<24
        begin

        truncate table #et_technical_raw
        commit

        set @filename_full=@filename_template || case when @hour_var<10 then '0' else '' end || cast(@hour_var as varchar(2)) || @filename_end
        commit

                /*
                To prevent data duplication, we only process the file if data is not already in
                */
                if not EXISTS(SELECT top 1 source_filename FROM et_technical WHERE source_filename = @filename_full)
                begin
                                set @sql = '
                                                 load table #et_technical_raw(
                                                                                                                 id''|'',
                                                                                                                 parameter_name''|'',
                                                                                                                 parameter_value''|'',
                                                                                                                 tstamp_raw''\n''
                                                 )
                                                 from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Leonardo/Technical/data/p=' || @filename_date || '/###filename_here###''
                                                 QUOTES OFF
                                                 ESCAPES OFF
                                                 NOTIFY 1000'
                                 --    SKIP 1
                                 commit

                                 execute(replace(@sql,'###filename_here###',@filename_full))

                                                                 /*
                                                                    check for errors: there will be load errors if the file is not there (some hourly data may be missing)
                                                                 */
                                 IF @@error = 0
                                 BEGIN
                                                                                                
                                                                                                commit

                                                                                                insert into et_technical (
                                                                                                        id
                                                                                                        ,parameter_name
                                                                                                        ,parameter_value
                                                                                                        ,tstamp
                                                                                                        ,reference_date
                                                                                                        ,source_filename
                                                                                                )
                                                select id
                                                 ,parameter_name
                                                 ,parameter_value
                                                 ,cast(substr(tstamp_raw,7,2)||'-'||substr(tstamp_raw,4,2)||'-'||substr(tstamp_raw,1,2)||' '||substr(tstamp_raw,10,12) as timestamp) as tstamp
                                                 ,@process_date as reference_date
                                                 ,@filename_full as source_filename
                                                 from
                                                 #et_technical_raw
                                                 commit
                                end -- end of if no error
                                else
                                select 'hour',@hour_var

        end -- if exist
        commit

        set @hour_var=@hour_var+1
        commit

end -- end of while
;
