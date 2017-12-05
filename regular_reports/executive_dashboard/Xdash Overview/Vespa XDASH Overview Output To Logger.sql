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
**Project Name:							OPS 2.0
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jose Loureda
**Stakeholder:                          Gavin Meggs
**Due Date:                             13/06/2014
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:
	
	A Lighter version for Xdash focused on panel performance based on dialling platform...

**Modules:

xdash_ov_output_to_logger

	This module provides output to a logging system
--------------------------------------------------------------------------------------------------------------
*/

create or replace procedure xdash_ov_output_to_logger              
        @output_msg             varchar(2000)
    ,@build_ID      bigint = NULL   -- Logger ID (so all builds end up in same queue)
        ,@logger_level          tinyint = 3  -- event level
as begin

        declare @output_with_timestamp varchar(2048)
        commit
        set @output_with_timestamp=cast(now() as varchar(19)) || ' ' || @output_msg
        commit

        if @build_ID is not null
        begin
                execute logger_add_event @build_ID, @logger_level, cast(@output_with_timestamp as varchar(200))
                        -- Check that there is data in viewing table
                commit --; --^^ to be removed
        end
        
        MESSAGE @output_with_timestamp TO CLIENT
        commit

-- check if the debug output table exists, if not create it.
        if not  exists  (
							select  1
							from    sysobjects
							where
											[name]                  =       'xdash_ov_debug_output_table'
									and uid                         =       user_id()
									and     upper([type])   =       'U'
						)
        begin
                                        -- table does not exist, let's create it!!!
        create table xdash_ov_debug_output_table (
                        row_id          int                     primary key identity -- primary key
                ,       msg                     varchar(2048)
        )
        commit

        end



        insert into xdash_ov_debug_output_table(msg)
        select @output_with_timestamp
        commit

end

commit;

grant execute on xdash_ov_output_to_logger to vespa_group_low_security;
commit;
