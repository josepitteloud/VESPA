/*###############################################################################
# Created on:   19/09/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for defining the current package of a Sky 
#		household
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 20/08/2012  TKD   v01 - initial version
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Process									    #####
-- ##############################################################################################################


CREATE TABLE #viq_current_pkg_tmp(
        ACCOUNT_NUMBER          varchar(20) default NULL,
        prem_sports             integer     default NULL,
        prem_movies             integer     default NULL,
        ent_cat_prod_start_dt   date        NOT NULL,
        Variety                 integer     default NULL,
        Knowledge               integer     default NULL,
        Kids                    integer     default NULL,
        Style_Culture           integer     default NULL,
        Music                   integer     default NULL,
        News_Events             integer     default NULL,
        rank                    integer     NOT NULL,
        Num_PremSports          tinyint     default 0,
        Num_PremMovies          tinyint     default 0,
        Num_Premiums            tinyint     default 0,
        Num_Mix                 tinyint     default 0,
        TV_Package              varchar(50) default 'UNKNOWN',
        Mix_Pack                varchar(20) default 'UNKNOWN'
);
commit;

INSERT INTO #viq_current_pkg_tmp (ACCOUNT_NUMBER, prem_sports, prem_movies,
        ent_cat_prod_start_dt, Variety, Knowledge, Kids, Style_Culture,
        Music, News_Events, rank)
SELECT ar.account_number
        --,effective_from_dt
        ,cel.prem_sports
        ,cel.prem_movies
        ,ent_cat_prod_start_dt
        ,cel.Variety
        ,cel.Knowledge
        ,cel.Kids
        ,cel.Style_Culture
        ,cel.Music
        ,cel.News_Events
        ,rank() over(partition by ar.account_number ORDER BY csh.effective_from_dt, csh.cb_row_id desc) as rank
--INTO --drop table
--        tempdb..viq_current_pkg_tmp
FROM VIQ_HH_ACCOUNT_TMP ar
        left join sk_prod.cust_subs_hist as csh
            on csh.account_number = ar.account_number
        inner join sk_prod.cust_entitlement_lookup as cel
            on csh.current_short_description = cel.short_description
WHERE csh.subscription_sub_type ='DTV Primary Viewing'
       AND csh.subscription_type = 'DTV PACKAGE'
       AND csh.status_code in ('AC','AB','PC')
       AND csh.effective_from_dt < today()
       AND csh.effective_to_dt   >=  today()
       AND csh.effective_from_dt != csh.effective_to_dt;
commit;
--9864424  Row(s) affected


DELETE FROM #viq_current_pkg_tmp WHERE rank > 1;
--5131   Row(s) affected
commit;


----add index
CREATE UNIQUE INDEX idx1 ON #viq_current_pkg_tmp(account_number);
commit;


--Add mix detail to the table
/*Alter table tempdb..viq_current_pkg_tmp add Num_PremSports tinyint default 0;
Alter table tempdb..viq_current_pkg_tmp add Num_PremMovies tinyint default 0;
Alter table tempdb..viq_current_pkg_tmp add Num_Premiums tinyint default 0;
Alter table tempdb..viq_current_pkg_tmp add Num_Mix tinyint default 0;
Alter table tempdb..viq_current_pkg_tmp add TV_Package   varchar(50) default 'UNKNOWN';
Alter table tempdb..viq_current_pkg_tmp add Mix_Pack     varchar(20) default 'UNKNOWN';
commit;
*/

--update new columns
UPDATE #viq_current_pkg_tmp a
SET
a.Num_PremSports = b.prem_sports,
a.Num_PremMovies = b.prem_Movies,
a.Num_Premiums = b.prem_sports + b.prem_Movies,
a.Num_Mix = (b.Variety + b.Knowledge + b.Kids + b.Style_Culture + b.Music + b.News_Events)
FROM #viq_current_pkg_tmp b
WHERE b.account_number = a.account_number;
commit;

-- this update depends on data derived by the previous update
UPDATE #viq_current_pkg_tmp a
SET
a.mix_pack=case
                when b.Num_Mix is null or b.Num_Mix=0                     then 'Entertainment Pack'
                when (b.variety=1 or b.style_culture=1)  and b.Num_Mix=1  then 'Entertainment Pack'
                when (b.variety=1 and b.style_culture=1) and b.Num_Mix=2  then 'Entertainment Pack'
                when b.Num_Mix > 0                                        then 'Entertainment Extra'
            end
FROM #viq_current_pkg_tmp b
WHERE b.account_number = a.account_number;
commit;

-- this update depends on data derived by the previous update
UPDATE #viq_current_pkg_tmp a
SET
a.TV_Package = case
                when b.prem_movies=2 and b.prem_sports=2 then 'Top Tier'
                when b.prem_movies=0 and b.prem_sports=2 then 'Dual Sports'
                when b.prem_movies=2 and b.prem_sports=0 then 'Dual Movies'
                when b.prem_movies=0 and b.prem_sports=1 then 'Single Sports'
                when b.prem_movies=1 and b.prem_sports=0 then 'Single Movies'
                when b.prem_movies>0 and b.prem_sports>0 then 'Other Premiums'
                when b.prem_movies=0 and b.prem_sports=0 and b.mix_pack = 'Entertainment Pack'  then 'Basic - Ent'
                when b.prem_movies=0 and b.prem_sports=0 and b.mix_pack = 'Entertainment Extra' then 'Basic - Ent Extra'
                end
FROM #viq_current_pkg_tmp b
WHERE b.account_number = a.account_number;
commit;

select top 100 * from #viq_current_pkg_tmp

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################

