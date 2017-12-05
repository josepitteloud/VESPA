/*
Simple procedure to save output tables from H2I run for further analysis/backup

Syntax:
		execute v289_backup_H2I_tables;
		execute v289_backup_H2I_tables 50;

*/


create or replace procedure v289_backup_H2I_tables
    @pc     int     =   NULL -- Enter the sample size here as appropriate
    as  begin
        
        if @pc is not null  
            begin

                message cast(now() as timestamp) || ' | Saving H2I tables for sample size : ' || cast(@pc as varchar) || '% ...' to client
				
				
				-- Declare variables
                declare @table_name     varchar(255)                                            commit --(^_^ )!
                declare @datestr        varchar(255)    = dateformat(now(),'_yyyymmdd_HHMMSS_') commit --(^_^ )!
                declare @sql_           varchar(255)                                            commit --(^_^ )!
                declare @sql2_          varchar(255)                                            commit --(^_^ )!
				declare @sql3_ 			varchar(255)											commit --(^_^ )!
                declare @i              int             = 0                                     commit --(^_^ )!

				message cast(now() as timestamp) || ' | ' || @datestr || cast(@pc as varchar) || 'pc' to client

				
				-- Loop over tables to back up
                while @i < 6    begin

                    -- Progress counter
					set @i = @i + 1
                    commit --(^_^ )!

					-- Define target table name and relevant copy and permissions commands
                    set @table_name =   case @i
                                            when    1   then    'v289_M06_dp_raw_data'
                                            when    2   then    'V289_M07_dp_data'
                                            when    3   then    'V289_M10_session_individuals'
                                            when    4   then    'V289_M10_combined_event_data'
                                            when    5   then    'V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING'
                                            when    6   then    'SC3I_Weightings'
                                        end
                    commit --(^_^ )!

                    set @sql_   = 'select * into ' + @table_name + @datestr + cast(@pc as varchar) + 'pc from ' + @table_name 
                    commit --(^_^ )!
                    
                    set @sql2_  = 'grant select on ' + @table_name + @datestr + cast(@pc as varchar) + 'pc to vespa_group_low_security'
                    commit --(^_^ )!

					-- Show SQL to client
                    set	@sql3_ =	'select ' + @sql_	+ ' union select ' + @sql2_	commit
					message cast(now() as timestamp) || ' | SQL : ' || @sql3_ to client

                    -- Execute SQL
                    execute(@sql_)
                    commit --(^_^ )!
                    execute(@sql2_)
                    commit --(^_^ )!
					
					
                end -- while
				
                message cast(now() as timestamp) || ' | Saving H2I tables for sample size : ' || cast(@pc as varchar) || '% ... DONE!' to client

            end -- if begin
        else
            message cast(now() as timestamp) || ' | Sample size input required. Exiting.' to client
        
    end -- procedure
commit
;
