--------------------------------------------------------------------------------------------------------------
/*
**Project Name:                     Toolbox
**Analysts:                         Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                          Vespa
**Stakeholder:                      Vespa Team
**Due Date:                         19/02/2013

**Business Brief:

    Accounts Returning Data Metric stands for, deriving for a range of dates all accounts of which all its boxes
    have return data. Derivation will be done on daily basis.

    IE: for a given date 18/11/2012 we have two accounts

    accounts    totalBoxes  Boxes That returned data on the 18/11/2012
        A           3                           2
        B           2                           2

    This metric will flag acount B as all its boxes returned data that day.

    same is applied for the rest of the days and then compiled together in the output table

**Sections:

    S01 - Initialising environment
    S02 - Deriving Metric
    S03 - Assembling output
    S04 - Managing privileges
*/
--------------------------------------------------------------------------------------------------------------

------------------------------------
/* S01 - Initialising environment */
------------------------------------
/*
if object_id('vespa_toolbox_acreturndata') is not null
    drop procedure vespa_toolbox_acreturndata;

commit;
go

declare @table__ varchar(50)
execute vespa_analysts.vespa_toolbox_acreturndata @histfrom,@histto,@toolbox_ap,@toolbox_tn output

select @table__
*/

create or replace procedure vespa_toolbox_acreturndata
    @datefrom       date        = null
    ,@dateto        date        = null
    ,@panel_section varchar(3)  = null
    ,@tablename     varchar(50) output
as begin

	MESSAGE cast(now() as timestamp)||' | Begining vespa_toolbox_acreturndata' TO CLIENT
	MESSAGE cast(now() as timestamp)||' | Begining S01 - Initialising environment' TO CLIENT

    -- Initialising local variables...
    declare @dkfrom integer
    declare @dkto   integer

    -- So this transformation here what it does is to change the shape of the date to the
    -- shape of the dimension values for dates used in the vespa tables, the intention here
    -- is to speed up results...
    set @dkfrom = convert(integer,dateformat(@datefrom,'yyyymmddhh'))       -- YYYYMMDD00
    set @dkto   = convert(integer,(dateformat(@dateto,'yyyymmdd')+'23'))    -- YYYYMMDD23

  if object_id('VESPA_ANALYSTS.transition_acreturndata')    is not null 
	begin
		drop table VESPA_ANALYSTS.transition_acreturndata
	end
	
	commit

	create table transition_acreturndata(
        thedate_        date
        ,account_number varchar(20)
        ,panel_id       tinyint
        ,box_count      tinyint
        ,expected_boxes tinyint
    )
	
	commit
	
	MESSAGE cast(now() as timestamp)||' | S01 - Initialising environment DONE' TO CLIENT
    ---------------------------
    /* S02 - Deriving Metric */
    ---------------------------
	
	MESSAGE cast(now() as timestamp)||' | Begining S02 - Deriving Metric' TO CLIENT
	
    -- We need to be able to provide a whole view of the Vespa Panels (12,6,7)
    -- and say what accounts where on what day on which panel, how many boxes did it has at that given date
    -- and how many of this boxes did actually returned any data...

    -- Collecting cases for Daily Panel from Viewing Events table...
    if lower(@panel_section) = 'dp'
        begin

			MESSAGE cast(now() as timestamp)||' | Begining S02 - Extracting DP' TO CLIENT
		
            insert  into transition_acreturndata
            select
                    cast(left(cast(thedate as varchar(10)),8) as date)  as thedate_
                    ,account_number
                    ,panel_id
                    ,count(distinct service_instance_id)                as box_count
                    ,convert(tinyint, null)                             as expected_boxes
            from    (
                        select  distinct
                                dk_event_start_datehour_dim                 as thedate
                                ,account_number
                                ,panel_id
                                ,service_instance_id
                        from    sk_prod.vespa_dp_prog_viewed_current
                        where   dk_event_start_datehour_dim between @dkfrom and @dkto
                        and     panel_id not in (4)
                        and     account_number is not null
                        and     service_instance_id is not null
                    ) as base
            group   by  thedate_
                        ,account_number
                        ,panel_id
			
			MESSAGE cast(now() as timestamp)||' | LINES AFFECTED: '|| @@ROWCOUNT TO CLIENT
			MESSAGE cast(now() as timestamp)||' | S02 - Extrating DP DONE' TO CLIENT

        end


    -- Collecting cases for Daily Panel from None Viewing Events table...
    /*
	
		[ad] 20140225 : Commenting this out as we no longer have visibility over non-viewing events...
	
		if lower(@panel_section) = 'dpn'
        begin

            insert  into transition_acreturndata
            select
                    cast(left(cast(thedate as varchar(10)),8) as date)  as thedate_
                    ,account_number
                    ,panel_id
                    ,count(distinct service_instance_id)                as box_count
                    ,convert(tinyint, null)                             as expected_boxes
            from    (
                        select  distinct
                                dk_event_start_datehour_dim                 as thedate
                                ,account_number
                                ,case when panel_id = 4 then 12 else 12 end as panel_id -- 4 is now 12
                                ,service_instance_id
                        from    sk_prod.vespa_dp_prog_non_viewed_current
                        where   dk_event_start_datehour_dim between @dkfrom and @dkto
                        and     panel_id in (4,12)
                        and     account_number is not null
                        and     service_instance_id is not null
                    ) as base
            group   by  thedate_
                        ,account_number
                        ,panel_id

        end
	*/

    -- Collecting cases for Alter Panels table...BC changed code to pull AP data from alt_panel_data table 20-05-2013. Alter panel table name changed to panel_table 23-09-2013
    if lower(@panel_section) = 'ap'
        begin

			MESSAGE cast(now() as timestamp)||' | Begining S02 - Extrating AP' TO CLIENT
		
			insert  into transition_acreturndata
            select  pd.dt                      as thedate_
					,sbv.account_number
					,pd.panel                 as panel_id
					,sum(pd.data_received)    as box_count
					,convert(tinyint, null)    as expected_boxes
			from 	vespa_analysts.panel_data 					as pd
					inner join VESPA_ANALYSTS.VESPA_SINGLE_BOX_VIEW as sbv
					on pd.subscriber_id = cast(sbv.subscriber_id as int)
			where   pd.dt between @datefrom and @dateto
			and		pd.panel in (6,7,5)
			and     sbv.account_number is not null
			and     pd.subscriber_id is not null
			group   by  sbv.account_number
                        ,pd.dt
                        ,pd.panel
			
			MESSAGE cast(now() as timestamp)||' | LINES AFFECTED: '|| @@ROWCOUNT TO CLIENT
			MESSAGE cast(now() as timestamp)||' | S02 - Extrating AP DONE' TO CLIENT
						
        end

    --commit
    -- Now to derive the total number of boxes that an account had at a given date
    -- we link # with cust service instance which holds history of customer and associated boxes...
    
	declare @date_start date
	declare @date_end date
	declare @iteration_date date
    set @date_start=(select min(thedate_) from transition_acreturndata)
    set @date_end=(select max(thedate_) from transition_acreturndata)
    set @iteration_date=@date_start

        WHILE @iteration_date <= @date_end
        BEGIN

        MESSAGE cast(now() as timestamp)||' S02 - processing day ' || cast(@iteration_date as varchar(20)) TO CLIENT

       select  base.thedate_
                                ,base.account_number
                                ,count(distinct csi.SRC_System_ID)      as totalboxes
                into    #AC_boxes
                from    transition_acreturndata                     as base
                                inner join SK_PROD.CUST_SERVICE_INSTANCE    as csi
                                on base.account_number = csi.account_number
                where   (base.thedate_ between csi.effective_from_dt and csi.effective_to_dt)
                                 and (base.thedate_=@iteration_date)
         group   by  base.thedate_
                                        ,base.account_number
		
-- #AC_boxes only contains data for one day
                update  transition_acreturndata
                set     expected_boxes = ab.totalboxes
                from    transition_acreturndata as aat
                                inner join #AC_boxes    as ab
                                on  aat.thedate_ = ab.thedate_
                                and aat.account_number = ab.account_number

 	  MESSAGE cast(now() as timestamp)||' | LINES AFFECTED: '|| @@ROWCOUNT TO CLIENT
      MESSAGE cast(now() as timestamp)||' S02 - processing day ' || cast(@iteration_date as varchar(20)) || ' done' TO CLIENT

                drop table #AC_boxes
                commit
                set @iteration_date=dateadd(dd,1,@iteration_date)
end

	
	MESSAGE cast(now() as timestamp)||' | S02 - Deriving Metric DONE' TO CLIENT
	
    -----------------------------
    /* S03 - Assembling output */
    -----------------------------

	MESSAGE cast(now() as timestamp)||' | Begining S03 - Assembling output' TO CLIENT
	
    -- Based on this metric we only want to flag those accounts having all its boxes transmitting
    -- data back to us, any account having a portion of its boxes interacting with the panels is not considered
    -- as a returning account...


    if  (
            select  count(1)
            from    transition_acreturndata
        ) >0
        begin

			MESSAGE cast(now() as timestamp)||' | @ S03: Constructing toolbox_t_acreturndata' TO CLIENT
			
            if object_id('vespa_analysts.toolbox_t_acreturndata') is not null
                begin
                    drop table vespa_analysts.toolbox_t_acreturndata
                end

            commit

            select  *
            into    vespa_analysts.toolbox_t_acreturndata
            from    transition_acreturndata
            where   box_count >= expected_boxes

            commit
			MESSAGE cast(now() as timestamp)||' | LINES AFFECTED: '|| @@ROWCOUNT TO CLIENT
			MESSAGE cast(now() as timestamp)||' | @ S03: Constructing toolbox_t_acreturndata DONE' TO CLIENT
			
            set @tablename = 'vespa_analysts.toolbox_t_acreturndata'

            grant select on vespa_analysts.toolbox_t_acreturndata to public
            drop table VESPA_ANALYSTS.transition_acreturndata
            commit

        end
    else
        set @tablename = 'Null'

	MESSAGE cast(now() as timestamp)||' | Begining S03 - Assembling output DONE' TO CLIENT

end;


-------------------------------
/* S04 - Managing privileges */
-------------------------------
grant execute on vespa_toolbox_acreturndata to vespa_group_low_security;

commit