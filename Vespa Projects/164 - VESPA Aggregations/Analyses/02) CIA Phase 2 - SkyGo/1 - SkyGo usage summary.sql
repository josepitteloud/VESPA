/*###############################################################################
# Created on:   05/11/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Summary table for SkyGo usage
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 1.0 - creating summary
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - sk_prod.
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 05/11/2013  SBE   Initial version
# 06/11/2013  MNG   Include calculation on measuring metric
# 19/11/2013  SBE   Change of definitions/requirements - script adjustment
#
###############################################################################*/


  -- ##############################################################################################################
  -- ##### STEP 0.1 - preparing environment                                                                   #####
  -- ##############################################################################################################
-- if object_id('VAggrAnal_SkyGo_Usage_Summary') is not null then drop table VAggrAnal_SkyGo_Usage_Summary end if;
create table VAggrAnal_SkyGo_Usage_Summary (
      Id                                  bigint            null     identity,
      Account_Number                      varchar(20)                default null,

      Vol_All                             bigint            null     default 0,

      Vol_All_Sports                      bigint            null     default 0,
      Vol_All_Movies                      bigint            null     default 0,
      Vol_All_Non_Premium                 bigint            null     default 0,

      Vol_Linear                          bigint            null     default 0,
      Vol_VOD                             bigint            null     default 0,
      Vol_DL                              bigint            null     default 0,

      Updated_On                          datetime          not null default timestamp,
      Updated_By                          varchar(30)       not null default user_name()
);

create unique hg index idx01 on VAggrAnal_SkyGo_Usage_Summary(Account_Number);
grant select on VAggrAnal_SkyGo_Usage_Summary to vespa_group_low_security;
grant select, delete, insert, update on VAggrAnal_SkyGo_Usage_Summary to ngm;



  -- ###############################################################################
  -- ##### Set up environment                                                  #####
  -- ###############################################################################
create variable @varStartDate date;
create variable @varEndDate date;

set @varStartDate = '2013-09-01';
set @varEndDate   = '2013-09-30';



-- ##############################################################################################################
-- ##### STEP 1.0 - creating summary                                                                        #####
-- ##############################################################################################################

truncate table bednaszs.VAggrAnal_SkyGo_Usage_Summary;

insert into bednaszs.VAggrAnal_SkyGo_Usage_Summary
       (Account_Number, Vol_All, Vol_All_Sports, Vol_All_Movies, Vol_All_Non_Premium, Vol_Linear, Vol_VOD, Vol_DL)

select

		Account_Number

		--## All Go
		,sum(
          case
            when x_usage_type = 'VOD' and is_progressive = 'N' and status in (1, 6, 7) then 1
            when x_usage_type <> 'VOD' or is_progressive <> 'N' then 1
              else 0
          end
        ) as Vol_All

		--## All Go Sports
		,sum(
			case
				when ( (x_usage_type <> 'VOD' or is_progressive <> 'N')
					AND broadcast_channel in ('StreamSkySports1'
											,'StreamSkySports2'
											,'StreamSkySports3'
											,'StreamSkySports4'
											,'StreamSkySportsFormula1'
											,'StreamSkySportsNews'
											,'Formula1'
											,'SkySports')
					) then 1
				when x_usage_type = 'VOD' and is_progressive = 'N' and status in (1,6,7)
					AND broadcast_channel in ('StreamSkySports1'
											,'StreamSkySports2'
											,'StreamSkySports3'
											,'StreamSkySports4'
											,'StreamSkySportsFormula1'
											,'StreamSkySportsNews'
											,'Formula1'
											,'SkySports') then 1
					else 0 end) as Vol_All_Sports

		--## All Go Movies
		,sum(case
			when ( (x_usage_type <> 'VOD' or is_progressive <> 'N')
				AND broadcast_channel in ('StreamMoviesActionAdventure'
												,'StreamMoviesComedy'
												,'StreamMoviesCrimeThriller'
												,'StreamMoviesDisney'
												,'StreamMoviesDramaRomance'
												,'StreamMoviesFamily'
												,'StreamMoviesGreats'
												,'StreamMoviesPremiere'
												,'StreamMoviesSciFiHorror'
												,'StreamMoviesSelect'
												,'StreamMoviesShowcase')
					) then 1
			when x_usage_type = 'VOD' and is_progressive = 'N' and status in (1,6,7)
				AND broadcast_channel in ('StreamMoviesActionAdventure'
												,'StreamMoviesComedy'
												,'StreamMoviesCrimeThriller'
												,'StreamMoviesDisney'
												,'StreamMoviesDramaRomance'
												,'StreamMoviesFamily'
												,'StreamMoviesGreats'
												,'StreamMoviesPremiere'
												,'StreamMoviesSciFiHorror'
												,'StreamMoviesSelect'
												,'StreamMoviesShowcase') then 1

			else 0 end) as Vol_All_Movies

		--## All Go Non Premium
		,sum(case
			when ( (x_usage_type <> 'VOD' or is_progressive <> 'N')
				AND broadcast_channel not in ('StreamSkySports1'
											,'StreamSkySports2'
											,'StreamSkySports3'
											,'StreamSkySports4'
											,'StreamSkySportsFormula1'
											,'StreamSkySportsNews'
											,'Formula1'
											,'SkySports'
											,'StreamMoviesActionAdventure'
											,'StreamMoviesComedy'
											,'StreamMoviesCrimeThriller'
											,'StreamMoviesDisney'
											,'StreamMoviesDramaRomance'
											,'StreamMoviesFamily'
											,'StreamMoviesGreats'
											,'StreamMoviesPremiere'
											,'StreamMoviesSciFiHorror'
											,'StreamMoviesSelect'
											,'StreamMoviesShowcase')
					) then 1
			when x_usage_type = 'VOD' and is_progressive = 'N' and status in (1,6,7)
				AND broadcast_channel not in ('StreamSkySports1'
											,'StreamSkySports2'
											,'StreamSkySports3'
											,'StreamSkySports4'
											,'StreamSkySportsFormula1'
											,'StreamSkySportsNews'
											,'Formula1'
											,'SkySports'
											,'StreamMoviesActionAdventure'
											,'StreamMoviesComedy'
											,'StreamMoviesCrimeThriller'
											,'StreamMoviesDisney'
											,'StreamMoviesDramaRomance'
											,'StreamMoviesFamily'
											,'StreamMoviesGreats'
											,'StreamMoviesPremiere'
											,'StreamMoviesSciFiHorror'
											,'StreamMoviesSelect'
											,'StreamMoviesShowcase') then 1
					else 0 end) as Vol_All_Non_Premium


		--## Vol_Linear
		,sum(case when (x_usage_type = 'Live Viewing'
					AND LOWER(site_url) NOT LIKE 'sm%'
					) then 1 else 0 end) as Vol_Linear

		--## Vol_VOD
		,sum(case when (x_usage_type = 'VOD'
					AND	is_progressive = 'Y'
					) then 1 else 0 end) as Vol_VOD


		--## Vol_download -- include all completed downloads and deleted completed downloads
		,sum(case when (status in (1, 6, 7)
					AND x_usage_type = 'VOD'
					AND is_progressive = 'N'
					) then 1 else 0 end) as Vol_DL

from  sk_prod.SKY_PLAYER_USAGE_DETAIL
where activity_dt between @varStartDate and @varEndDate and account_number is not null
group by Account_Number;
commit;







  -- ##############################################################################################################








