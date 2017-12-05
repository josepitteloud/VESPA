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
**Project Name:                                         Vespa Executive Dashboard
**Analysts:                                                     Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                                                      Jose Loureda
**Stakeholder:                                          Tone Mooney
**Due Date:                                                     22/02/2013
**Project Code (Insight Collation):     
**Sharepoint Folder:                            http://rtci/Sky%20Projects/Forms/AllItems.aspx?RootFolder=%2fSky%20Projects%2fVespa%2fRegular%20reports
                                                                        %2fMeta%2fExecutive%20Dashboard&FolderCTID=&View={95B15B22-959B-4B62-809A-AD43E02001BD}
                                                                        
**Business Brief:

Creating Tables required for the XDASH...

** List of tables involved in the project:

+       vespa_xdash_tasks                                       <Managed by Project>
+       vespa_xdash_hist_acinteractionkpi       <Managed by Project>
+       vespa_xdash_stage_adhocmetrics          <Managed by Project>
+       vespa_xdash_o1_adhocmetrics                     <Managed by Project>
+       vespa_xdash_o2_histviewman                      <Managed by Project>
+       vespa_xdash_o3_trafficlights            <Managed by Project>
+       vespa_xdash_o4_dpdreturnextract         <Managed by Project>
+       vespa_xdash_o5_vespaPanelBalance        <Managed by Project>
+       vespa_analysts.vespa_OpDash_hist_optout
+       vespa_analysts.vespa_panman_hist_summary
+       vespa_analysts.vespa_PanMan_02_vespa_panel_overall
+       vespa_analysts.vespa_PanMan_03_panel_6_overall
+       vespa_analysts.vespa_PanMan_04_panel_7_overall
+       vespa_analysts.vespa_PanMan_09_traffic_lights
+       vespa_analysts.Vespa_PanMan_all_aggregated_results
+       vespa_analysts.Vespa_PanMan_hist_trafficlight
+       vespa_analysts.vespa_OpDash_01_OptOut_AllAccounts
+       vespa_analysts.vespa_sbv_hist_qualitycheck
+       vespa_analysts.sc2_metrics
+       vespa_analysts.sc2_segments_lookup
+       vespa_analysts.sc2_weightings
+       vespa_analysts.SC2_Sky_base_segment_snapshots
+       sk_prod.sky_calendar
+       sk_prod.vespa_dp_prog_viewed_current
+       sk_prod.vespa_dp_prog_non_viewed_current
+       sk_prod.vespa_ap_prog_viewed_current
+       SK_PROD.CUST_SERVICE_INSTANCE

*/

-- 00) vespa_xdash_tasks

if object_id('vespa_xdash_tasks') is not null
        drop table vespa_xdash_tasks;
        
commit;
go

create table vespa_xdash_tasks(
        sequencer       integer         default autoincrement
        ,task           varchar(15) not null
        ,status         bit             default 0
        ,weekending     date            
        ,audit_date     date            not null
)

commit;
go

insert  into vespa_xdash_tasks(task,audit_date) Values('histviewman',cast(now() as date));
insert  into vespa_xdash_tasks(task,audit_date) Values('adhocs',cast(now() as date));
insert  into vespa_xdash_tasks(task,audit_date) Values('trafficlights',cast(now() as date));
insert  into vespa_xdash_tasks(task,audit_date) Values('dpdreturn',cast(now() as date));
insert  into vespa_xdash_tasks(task,audit_date) Values('vespabalance',cast(now() as date));

commit;
go


-- 01) vespa_xdash_hist_acinteractionkpi

if object_id('vespa_xdash_hist_acinteractionkpi') is not null
        drop table vespa_xdash_hist_acinteractionkpi;
        
commit;
go

create table vespa_xdash_hist_acinteractionkpi(
		weekending          date
		,account_number     varchar(20)
        ,panel_id                       tinyint
        ,box_count                      tinyint
		,expected_boxes         tinyint
        ,reporting_quality      real
);

create date index some_date_index       on vespa_xdash_hist_acinteractionkpi(weekending);
create hg       index some_hg_index     on vespa_xdash_hist_acinteractionkpi(account_number);


commit;
go

-- 02) vespa_xdash_o1_adhocmetrics

if object_id('vespa_xdash_o1_adhocmetrics') is not null
    drop table vespa_xdash_o1_adhocmetrics;

commit;
go

create table vespa_xdash_o1_adhocmetrics (
        metric          varchar(20)     not null unique
    ,index_     integer                 default autoincrement
        ,lastWeek       decimal(15,3)   default 0
        ,lastMonth      decimal(15,3)   default 0 
);
commit;
go

insert into vespa_xdash_o1_adhocmetrics(metric) values ('dp_vespa_consent');
insert into vespa_xdash_o1_adhocmetrics(metric) values ('dp_vespa_consent_per');
insert into vespa_xdash_o1_adhocmetrics(metric) values ('dp_ac_enabled');
insert into vespa_xdash_o1_adhocmetrics(metric) values ('dp_ac_returning');
insert into vespa_xdash_o1_adhocmetrics(metric) values ('dp_avg_ac_ret_ok');
insert into vespa_xdash_o1_adhocmetrics(metric) values ('dp_avg_ac_ret_notok');
insert into vespa_xdash_o1_adhocmetrics(metric) values ('dp_avg_ac_return');
insert into vespa_xdash_o1_adhocmetrics(metric) values ('dp_ac_reliable');
insert into vespa_xdash_o1_adhocmetrics(metric) values ('dp_ess'); -- Effective Sample Size
insert into vespa_xdash_o1_adhocmetrics(metric) values ('all_ac_enabled');
insert into vespa_xdash_o1_adhocmetrics(metric) values ('all_ac_returning');
insert into vespa_xdash_o1_adhocmetrics(metric) values ('all_avg_ac_ret_ok');
insert into vespa_xdash_o1_adhocmetrics(metric) values ('all_avg_ac_ret_notok');
insert into vespa_xdash_o1_adhocmetrics(metric) values ('all_avg_ac_return');
insert into vespa_xdash_o1_adhocmetrics(metric) values ('all_ac_reliable');
insert into vespa_xdash_o1_adhocmetrics(metric) values ('adsm_hhs_1box'); -- Start of adsmart (cortb 06/11/2013)
insert into vespa_xdash_o1_adhocmetrics(metric) values ('adsm_hhs_mt1box');
insert into vespa_xdash_o1_adhocmetrics(metric) values ('adsm_hhs_all_adsm');
insert into vespa_xdash_o1_adhocmetrics(metric) values ('non_adsm_hhs');
insert into vespa_xdash_o1_adhocmetrics(metric) values ('adsm_hhs_reporting');
insert into vespa_xdash_o1_adhocmetrics(metric) values ('vp_ac_enabled'); -- Virtual Panel 
insert into vespa_xdash_o1_adhocmetrics(metric) values ('vp_ac_returning');
insert into vespa_xdash_o1_adhocmetrics(metric) values ('all_tacc_ac_enabled'); -- Turnaround
insert into vespa_xdash_o1_adhocmetrics(metric) values ('all_tacc_ac_return');
insert into vespa_xdash_o1_adhocmetrics(metric) values ('all_tacc_ac_ret_ge50');	-- with RQ>=0.5
insert into vespa_xdash_o1_adhocmetrics(metric) values ('all_ac_ret_ge50');	-- with RQ>=0.5
commit;
go

-- 03) vespa_xdash_o2_histviewman

if object_id('vespa_xdash_o2_histviewman') is not null
    drop table vespa_xdash_o2_histviewman;

commit;
go

create table vespa_xdash_o2_histviewman (
        weekending              date
		,account_number     varchar(20)
        ,panel_id                       tinyint
        ,box_count                      tinyint
        ,expected_boxes         tinyint
        ,reporting_quality      real
);

create date index some_date_index2      on vespa_xdash_o2_histviewman(weekending);
create hg       index some_hg_index2    on vespa_xdash_o2_histviewman(account_number);

commit;
go

-- 04) vespa_xdash_o3_trafficlights

if object_id('vespa_xdash_o3_trafficlights') is not null
        drop table vespa_xdash_o3_trafficlights;

commit;
go

create table vespa_xdash_o3_trafficlights(
        variable_name           varchar(25)             unique not null
        ,vespa_imbalance        decimal(15,2)   not null
        ,avg_convergence        decimal(15,2)   not null
        ,std_convergence        decimal(15,2)   not null                                
);

commit;
go


-- 05) vespa_xdash_o4_dpdreturnextract

if object_id('vespa_xdash_o4_dpdreturnextract') is not null
        drop table vespa_xdash_o4_dpdreturnextract;

commit;
go

create table vespa_xdash_o4_dpdreturnextract(
        weekending                      date    not null
        ,sky_week                       varchar(7) not null
        ,reliably_returning     integer
        ,returning_data         integer
        ,enabled                        integer
);

commit;
go


-- 06) vespa_xdash_o5_vespaPanelBalance

if object_id('vespa_xdash_o5_vespaPanelBalance') is not null
        drop table vespa_xdash_o5_vespaPanelBalance;

commit;
go

create table vespa_xdash_o5_vespaPanelBalance(
        sky_week                varchar(7) not null
        ,sky_base               integer
        ,convergence    decimal (15,3)
        ,Pop_coverage   decimal (15,3)
        ,Seg_coverage   decimal (15,3)
);

commit;
go


-- 07) vespa_xdash_o6_vespaPanelBalanceHist

if object_id('vespa_xdash_o6_vespaPanelBalanceHist') is not null
        drop table vespa_xdash_o6_vespaPanelBalanceHist;

commit;
go

create table vespa_xdash_o6_vespaPanelBalanceHist(
        weekending              date    not null
        ,balance_index  decimal (15,3)
);

commit;
go

-------------------------
/* Granting Privileges */
-------------------------

grant select on vespa_xdash_o1_adhocmetrics                     to vespa_group_low_security;
grant select on vespa_xdash_o2_histviewman                              to vespa_group_low_security;
grant select on vespa_xdash_o3_trafficlights                    to vespa_group_low_security;
grant select on vespa_xdash_o4_dpdreturnextract                 to vespa_group_low_security;
grant select on vespa_xdash_o5_vespaPanelBalance                to vespa_group_low_security;
grant select on vespa_xdash_o6_vespaPanelBalanceHist    to vespa_group_low_security;
grant select on vespa_xdash_hist_acinteractionkpi               to vespa_group_low_security;
grant select on vespa_xdash_tasks                                               to vespa_group_low_security;

commit;
go


