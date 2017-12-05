 /*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:                         DB Maintenance Report (DBM)
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Stakeholder:                          SkyIQ - Gavin Meggs / Jose Loureda
**Due Date:                             14/02/2014
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:

        A report to monitor the DB space usage related to SkyIQ DB users...
        
        This is a report that is highly recommended to be ran out of office hours, where there is little interaction
        with the tables, hence getting a more accurate snapshot of the DB. Mind that such interaction could disrupt
        the execution of the report...

**Sections:

        S0  -    Declaring Variables
        S1  -    Targeting Work Load (Users List)
        S2A -    Targeting Tables Amended since report last run
        S2  -    Targeting Tables per User
        S3  -    Weighting the Tables
        S4  -    Housekeeping

--------------------------------------------------------------------------------------------------------------
*/


create or replace procedure dsd_dbmaintenance_report
	@bypass bit = 0
as begin

	---------------------------
	-- S0 - Declaring Variables
	---------------------------

	declare @uname      varchar(50)
	declare @list_date  date
	declare @uname_     varchar(50)
	declare @tname      varchar(100)
	declare @updated    timestamp
	declare @mbytes     decimal(16,5)
	declare @sql_ 		varchar(3000)
	declare @count      integer




	----------------------------------------
	-- S1 - Targeting Work Load (Users List)
	----------------------------------------

	-- so we build the users list due to two conditions 1. Either the table doesn't exist or,
	-- 2. we are executing the report and there is an old batch in place...

	if object_id('users_list') is not null -- TAL (user should not be fixed)
		begin

			-- if running dates are different we need to refresh the list of users...
			
			if @bypass = 0 	-- This bypass for when DB kills the overnight run... 
							-- for these cases we don't need to start from cero again next morning...
			begin
			
				select @list_date = min(exe_dt) from users_list

				if  (@list_Date <> cast(now() as date))
				begin

					drop table users_list
					truncate table vespa_dbmaintenance_summary
					commit
				
					-- if the table doesn't exist then we need to re build it
					select  distinct
							member_name
							,row_number() over  (
													order by    member_name
												)   as row_id
							,0                      as done_flag
							,cast(now() as date)    as exe_dt
					into    users_list
					from    sys.sysgroups
					where   lower(member_name) NOT LIKE 'sk_%'
					AND     member_name NOT IN ('DBA','SYS','dbo','sysbadmin','sybadmin','rs_systabgroup')

					commit

				end
				
			end
		end
	else
		begin

			-- if the table doesn't exist then we need to re build it
			select  distinct
					member_name
					,row_number() over  (
											order by    member_name
										)   as row_id
					,0                      as done_flag
					,cast(now() as date)    as exe_dt
			into    users_list
			from    sys.sysgroups
			where           lower(member_name) NOT LIKE 'sk_%'
			AND     member_name NOT IN ('DBA','SYS','dbo','sysbadmin','sybadmin','rs_systabgroup')

			truncate table vespa_dbmaintenance_summary
			commit
			
		end

	commit


	-------------------------------------------------------
	-- S2A - Targeting Tables Amended since report last run
	-------------------------------------------------------

	if object_id('processing_table') is not null
		drop table processing_table

	commit

	-- collect all tables and latest update time on DB
	SELECT  trim(base.user_name) as uname
			,trim(base.table_name) as tname
			,case when sysiqtable.update_time < '1900-01-01 00:00:00.000' then '1900-01-01 00:00:00.000' else sysiqtable.update_time end as update_time
	into    processing_table
	FROM    (
				select  systable.table_id
						,systable.table_name
						,sysuser.user_name
				from    sys.systable
						inner join sys.sysuser
						on  systable.creator = sysuser.user_id
				where   lower(user_name) NOT LIKE 'sk_%'
				AND     user_name NOT IN ('DBA','SYS','dbo','sysbadmin','sybadmin','rs_systabgroup')
			)   as base
			inner join sys.sysiqtable
			on  base.table_id = sysiqtable.table_id

	commit


	---------------------------------
	-- S2 - Targeting Tables per User
	---------------------------------

	-- hence, lets see if there is anyone still pending for processing...
	while   exists  (
						select  first member_name
						from    users_list
						where   done_flag = 0
					)
	begin

		-- if so, lets get the name for that user...
		select @uname = member_name
		from    users_list
		where   row_id = (select min(row_id) from users_list where done_flag = 0)

		set @count = 1

		----------------------------
		-- S3 - Weighting the Tables
		----------------------------
			
		begin transaction

			declare cursor1 cursor for

				SELECT  uname, tname, update_time
				FROM    processing_table
				where	uname = @uname

			for read only

			open cursor1
			fetch   next cursor1 into @uname_,@tname,@updated

			while   (sqlstate = 0)
			begin
					
				if @tname <> 'v250_loop_counter02' -- Nasty patch, this table was causing troubles...
				begin
				
					set @sql_ = 'select @mbytes = cast((kbytes/1024) as decimal(16,2)) from sp_iqtablesize('''||@uname_||'.[' ||@tname || ']'')'
					execute(@sql_)
					set @sql_ = 'insert	into vespa_dbmaintenance_summary (uname,tname,updated,mbytes)'||
								'select '''||@uname_||''''||
										','''||@tname||''''||
										','''||@updated||''''||
										', '||@mbytes

					execute(@sql_)
					message now()||'| '||trim(@uname)||' '||@count to client
					set @count = @count + 1
					
				end
				
				fetch   next cursor1 into @uname_,@tname,@updated

			end

			deallocate cursor1

		commit transaction

		update  users_list
		set     done_flag = 1
		where   member_name = @uname

		commit

	end


	--------------------
	-- S4 - Housekeeping
	--------------------

	-- if there is no-one else left for processing then lets just drop the list as we don't need it anymore...
	drop table users_list
	drop table processing_table
	commit


end;
commit;

grant execute on dsd_dbmaintenance_report to vespa_group_low_security;
commit;