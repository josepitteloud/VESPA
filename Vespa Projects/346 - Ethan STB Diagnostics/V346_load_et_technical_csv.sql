create or replace variable @input_file_path varchar(256) = '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/tanghoi/Diagnostics/all_Diagnostics_data.csv';


 
--------------------
-- Undelimited raw data
--------------------

drop table et_technical_undelimited;

create table et_technical_undelimited	(
												str				varchar(4906)
										)
;


-- Read the data
create or replace variable @sql_ varchar(4906);
set	@sql_	=	'
load table et_technical_undelimited	(
											str				''\n''
									)
from ''' || @input_file_path || '''
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
'
;

execute (@sql_);





--------------------
-- Parse into known fields
--------------------

create or replace variable @dummy_str varchar(255)	=	'#!"£$'
;


drop table #tmp;
create table #tmp	(
							str					varchar(8192)
                    	,	a					int
                        ,	ID					varchar(20)
                    	,	str_a				varchar(8192)
                    	,	b					int
                        ,	parameter_name		varchar(4906)
                    	,	str_b				varchar(8192)
                    	,	c					int
                        ,	parameter_value		varchar(4906)
                    	,	str_c				varchar(8192)
                        ,	tstamp				timestamp
                    )
;

insert into #tmp
select	--top 20
		str_replace(str,'#|',@dummy_str)			str_

    ,   charindex('|',str_)      				a
    ,   substring(str_,1,a-1)        			ID
    ,	substring(str_,a+1)						str_a

    ,   charindex('|',str_a)      				b
    ,   substring(str_a,1,b-1)        			parameter_name
    ,	substring(str_a,b+1)					str_b

    ,   charindex('|',str_b)			      				c
    ,   str_replace(substring(str_b,1,c-1),@dummy_str,'|')		parameter_value
    ,	substring(str_b,c+1)								str_c

    ,	cast(substr(str_c,7,2)||'-'||substr(str_c,4,2)||'-'||substr(str_c,1,2)||' '||substr(str_c,10,12) as timestamp) as tstamp

from	et_technical_undelimited
-- where	length(str) >	2000
;




-- Create final table if it does not exist already
IF NOT EXISTS	(
					SELECT	1
					FROM	SYSOBJECTS
					WHERE
							[NAME]			=	'et_technical'
						AND UID				=	USER_ID()
						AND	UPPER([TYPE])	=	'U'
				)
	BEGIN
	 
		create table et_technical	(
											row_id			int				primary key identity
										,	id				varchar(20)
										,	parameter_name	varchar(4906)
										,	parameter_value	varchar(4906)
										,	tstamp			timestamp
										,	replication		int
										,	audit_timestamp	timestamp
										-- ,	source_filename	varchar(100)
									)
		commit

		create lf index idx1 on et_technical(id)
		commit
		create wd index idx2 on et_technical(parameter_name)
		commit
		create lf index idx3 on et_technical(replication)
		commit
		create dttm index idx4 on et_technical(tstamp)
		commit
		create dttm index idx5 on et_technical(audit_timestamp)
		commit

	END
;


-- Upload into final permanent table
insert into et_technical	(
									id
								,	parameter_name
								,	parameter_value
								,	tstamp
								,	replication
								,	audit_timestamp
							)
select
		id
    ,	parameter_name
    ,	parameter_value
    ,	tstamp
    ,	count(1)
	,	now()	as	dt
from	#tmp
group by
		id
    ,	parameter_name
    ,	parameter_value
    ,	tstamp
	,	dt
;


drop table #tmp
;
