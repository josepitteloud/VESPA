create or replace procedure	V346_loadEthanDiagnosticsCsv
										@target_date		date	=	dateadd(day,-1,today())
as begin
										
	
	MESSAGE cast(now() as timestamp) || ' | ' || 'V346_loadEthanDiagnosticsCsv @target_date = ' || @target_date
	TO CLIENT

	--------------------
	-- Initialise
	--------------------

	MESSAGE cast(now() as timestamp) || ' | ' || 'Initialising environment'
	TO CLIENT

	-- Define target file name on ETL server
	declare @master_path varchar(256)
	commit
	
	set @master_path	=	'/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/tanghoi/Diagnostics/'
	commit

	declare @input_file_path varchar(256)
	commit

	
	select
			row_num
		,	'Ethan_BDC_STB_Parameters_' || dateformat(@target_date,'YYYYMMDD') || right('0' || cast(row_num as varchar(2)),2) || '0000.csv'	as	filename
	into    #filenames
	from	sa_rowgenerator(0,23)
	commit
	
	declare @i int = 0
	commit
	
	while	@i	<=	(select max(row_num) from #filenames)
	begin
	
		set @input_file_path = @master_path || (select filename from #filenames where row_num = @i)
		commit

		
		MESSAGE cast(now() as timestamp) || ' | ' || 'Importing file: ' || @input_file_path
		TO CLIENT


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

				create hg index idx1 on et_technical(id)
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



		--------------------
		-- Undelimited raw data
		--------------------

		MESSAGE cast(now() as timestamp) || ' | ' || 'Load raw undelimited data'
		TO CLIENT


		drop table et_technical_undelimited
		commit

		create table et_technical_undelimited	(
														str				varchar(4906)
												)
		commit


		-- Read the data
		declare @sql_ varchar(4906)
		commit
		
		set	@sql_	=	'
		load table et_technical_undelimited	(
													str				''\n''
											)
		from ''' || @input_file_path || '''
		QUOTES OFF
		ESCAPES OFF
		NOTIFY 1000
		'
		commit

		execute (@sql_)
		commit


		MESSAGE cast(now() as timestamp) || ' | ' || 'Load raw undelimited data...DONE. ' || @@rowcount
		TO CLIENT



		--------------------
		-- Parse into known fields
		--------------------

		MESSAGE cast(now() as timestamp) || ' | ' || 'Parse into known fields'
		TO CLIENT


		-- Dummy variable to replace #| combinations so as not to mistake them as delimiters
		declare @dummy_str varchar(255)	=	'#!"£$'
		commit


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
		commit

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
		where	parameter_name	like	'Device.Services.STBService.Components.FrontEnd.X_SKY_COM_DVBS.Tuner.%'
		-- where	length(str) >	2000
		commit


		MESSAGE cast(now() as timestamp) || ' | ' || 'Parse into known fields...DONE. ' || @@rowcount
		TO CLIENT


		-- Upload into final permanent table
		MESSAGE cast(now() as timestamp) || ' | ' || 'Load data into final table'
		TO CLIENT

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
		commit


		MESSAGE cast(now() as timestamp) || ' | ' || 'Load data into final table..DONE. ' || @@rowcount
		TO CLIENT


		drop table #tmp
		commit


		MESSAGE cast(now() as timestamp) || ' | ' || 'V346_loadEthanDiagnosticsCsv Finish!'
		TO CLIENT
		
		
		set @i = @i + 1
		commit

	end

	
end;	-- procedure
commit;

grant execute on V346_loadEthanDiagnosticsCsv to vespa_group_low_security;
commit;

