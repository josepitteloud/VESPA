
---Project V078 - Vespa Household to Individual---

---5th Aug 2012 - Currently waiting for New Kantar survey data so in meantime using Survey data loaded late Feb 2011--
---Currently sits on Old Server (Prod 10 so copying it across)
---Having to use vespa_analysts schema to copy across but will then log in to Prod 4 and create copy using dbarnett schema

---Copy Feb 2011 Survey over to New Server

---Create empty table and then copy data over;
--drop table dbarnett.VESPA_KANTAR_SURVEY_DATA_2011_02;commit;
create table vespa_analysts.VESPA_KANTAR_SURVEY_DATA_2011_02
(
  datevalid                      varchar(50)             null
 ,hhno                           varchar(50)             null
 ,individ                        varchar(50)             null
 ,customerid                     varchar(50)             null
 ,estabhomeno                    varchar(50)             null
 ,cardholderid                   varchar(50)             null
 ,wphouseholdnumber              varchar(50)             null
 ,reportedondatadate             varchar(50)             null
 ,sex                            varchar(50)             null
 ,age                            varchar(50)             null
 ,hw_status                      varchar(50)             null
 ,terminal_age_of_education      varchar(50)             null
 ,employment_status              varchar(50)             null
 ,tv_viewing_weekday             varchar(50)             null
 ,tv_viewing_saturday            varchar(50)             null
 ,tv_viewing_sunday              varchar(50)             null
 ,sky_subscriber                 varchar(50)             null
 ,number_of_tvs_in_home          varchar(50)             null
 ,cable_tv                       varchar(50)             null
 ,satellite_tv                   varchar(50)             null
 ,social_class                   varchar(50)             null
 ,lifecycle                      varchar(50)             null
 ,freeview_received_in_home      varchar(50)             null
 ,cinema_website_occasionally    varchar(50)             null
 ,tv_website_occasionally        varchar(50)             null
 ,football_website_occasionally  varchar(50)             null
 ,other_sport_website_occasional varchar(50)             null
 ,motoring_website_occasionally  varchar(50)             null
 ,social_networking_website_occa varchar(50)             null
 ,video_website_occasionally     varchar(50)             null
 ,coach_tour                     varchar(50)             null
 ,golfing                        varchar(50)             null
 ,skiing                         varchar(50)             null
 ,caravanning                    varchar(50)             null
 ,cruise                         varchar(50)             null
 ,safari                         varchar(50)             null
 ,city_break                     varchar(50)             null
 ,iplayer                        varchar(50)             null
 ,itv_player                     varchar(50)             null
 ,four_on_demand                 varchar(50)             null
 ,demand_five                    varchar(50)             null
 ,sky_player                     varchar(50)             null
 ,you_tube                       varchar(50)             null
 ,social_networks                varchar(50)             null
 ,web_portals                    varchar(50)             null
 ,watch_live_tv_on_pc            varchar(50)             null
 ,watch_prev_broadcast_tv_on_pc  varchar(50)             null
 ,dload_prog_to_pc_watch_later   varchar(50)             null
 ,theatre                        varchar(50)             null
 ,arts                           varchar(50)             null
 ,gaming                         varchar(50)             null
 ,gambling_at_a_bookmaker        varchar(50)             null
 ,gambling_via_a_newsagent       varchar(50)             null
 ,gambling_on_line               varchar(50)             null
 ,gamb_via_interactive_bookmaker varchar(50)             null
 ,gambling_via_telephone         varchar(50)             null
 ,british_gas                    varchar(50)             null
 ,edf_energy                     varchar(50)             null
 ,npower                         varchar(50)             null
 ,sainsburys_energy              varchar(50)             null
 ,southern_electric              varchar(50)             null
 ,other_gas_electricity_supplier varchar(50)             null
 ,homecare_with_british_gas      varchar(50)             null
 ,boiler_cover                   varchar(50)             null
 ,breakdown_cover                varchar(50)             null
 ,insurance_cover                varchar(50)             null
 ,boiler_care_plan               varchar(50)             null
 ,emergency_cover                varchar(50)             null
 ,no_utilities_cover             varchar(50)             null
 ,statements_apply_to_home       varchar(50)             null
 ,the_sun_news_of_the_world      varchar(50)             null
 ,the_mirror_sunday_mirror       varchar(50)             null
 ,the_express_sunday_express     varchar(50)             null
 ,daily_star_daily_star_sunday   varchar(50)             null
 ,daily_mail_mail_on_sunday      varchar(50)             null
 ,daily_telgraph_sunday_telgraph varchar(50)             null
 ,the_guardian_the_observer      varchar(50)             null
 ,the_independ_independ_on_sun   varchar(50)             null
 ,the_times_the_sunday_times     varchar(50)             null
 ,sony_playstation_2             varchar(50)             null
 ,sony_playstation_3             varchar(50)             null
 ,microsoft_xbox_1               varchar(50)             null
 ,microsoft_xbox_360             varchar(50)             null
 ,dating_websites_last_4_wks     varchar(50)             null
 ,dating_websites_last_6_months  varchar(50)             null
 ,online_auction_site_last_4wks  varchar(50)             null
 ,online_auction_site_last_6wks  varchar(50)             null
 ,online_bingo_last_4_wks        varchar(50)             null
 ,online_bingo_last_6_months     varchar(50)             null
 ,price_comparison_last_4_wks    varchar(50)             null
 ,price_comparison_last_6_months varchar(50)             null
 ,freq_buy_rent_video_games      varchar(50)             null
 ,e_on                           varchar(50)             null
 ,buy_bed_next_12mth             varchar(50)             null
 ,buy_sofa_armchair_next_12mth   varchar(50)             null
 ,buy_dining_suite_next_12mth    varchar(50)             null
 ,buy_cooker_next_12mth          varchar(50)             null
 ,buy_microwave_oven_next_12mth  varchar(50)             null
 ,buy_fridge_freezer_next_12mth  varchar(50)             null
 ,buy_washing_tumble_next_12mth  varchar(50)             null
 ,buy_dishwasher_next_12mth      varchar(50)             null
 ,buy_vacuum_cleaner_next_12mth  varchar(50)             null
 ,buy_no_new_stated_items_12mth  varchar(50)             null
 ,childrens_toys_games_high_st   varchar(50)             null
 ,childrens_toys_games_online    varchar(50)             null
 ,sportswear_high_st             varchar(50)             null
 ,sportswear_online              varchar(50)             null
 ,stout_in_home                  varchar(50)             null
 ,stout_out_of_home              varchar(50)             null
 ,cider_in_home                  varchar(50)             null
 ,cider_out_of_home              varchar(50)             null
 ,none_of_websites_last_4wks     varchar(50)             null
 ,none_of_websites_last_6mths    varchar(50)             null
 ,none_of_products_hightst_in3mt varchar(50)             null
 ,none_of_products_online_in3mth varchar(50)             null
 ,no_alcohol_purchased_in_home   varchar(50)             null
 ,no_alcohol_purchased_out_of_hm varchar(50)             null
 ,no_life_events_in_next_12_mnth varchar(50)             null
 ,espn_sport                     varchar(50)             null
 ,active_espn                    varchar(50)             null
 ,number_of_tvs_receiving_sky_di varchar(50)             null
 ,number_of_sky_digiboxes        varchar(50)             null
 ,sky_digibox_in_lounge_living_r varchar(50)             null
 ,sky_digibox_in_main_bedroom    varchar(50)             null
 ,sky_digibox_in_kids_bedroom    varchar(50)             null
 ,sky_digibox_in_kitchen         varchar(50)             null
 ,sky_digibox_in_study_office    varchar(50)             null
 ,sky_digibox_in_other_room      varchar(50)             null
 ,frequency_renting_dvds         varchar(50)             null
 ,watch_sky_on_outdoor_screen    varchar(50)             null
 ,reason_for_subscribing_to_sky  varchar(50)             null
 ,film                           varchar(50)             null
 ,pret_a_manger                  varchar(50)             null
 ,greggs                         varchar(50)             null
 ,laser_eye_care                 varchar(50)             null
 ,blu_ray_dvd_player             varchar(50)             null
 ,dvd_player                     varchar(50)             null
 ,nintendo_ds                    varchar(50)             null
 ,plasma_screen_tv               varchar(50)             null
 ,widescreen                     varchar(50)             null
 ,dvd_recorder                   varchar(50)             null
 ,video_recorder                 varchar(50)             null
 ,video_camera_camcorder         varchar(50)             null
 ,digital_camera                 varchar(50)             null
 ,mp3_i_pod                      varchar(50)             null
 ,pc_or_apple_mac                varchar(50)             null
 ,dial_up_connection             varchar(50)             null
 ,broadband                      varchar(50)             null
 ,sony_playstation2              varchar(50)             null
 ,microsoft_xbox_2               varchar(50)             null
 ,other_games_console            varchar(50)             null
 ,none_in_home                   varchar(50)             null
 ,number_of_dvd_players_in_home  varchar(50)             null
 ,frequency_of_renting_dvds_or_v varchar(50)             null
 ,music_live                     varchar(50)             null
 ,music_pre_recorded             varchar(50)             null
 ,theatre_and_arts               varchar(50)             null
 ,football                       varchar(50)             null
 ,rugby_union                    varchar(50)             null
 ,rugby_league                   varchar(50)             null
 ,cricket                        varchar(50)             null
 ,golf                           varchar(50)             null
 ,tennis                         varchar(50)             null
 ,horse_racing                   varchar(50)             null
 ,dog_racing                     varchar(50)             null
 ,motorsport                     varchar(50)             null
 ,snooker_pool                   varchar(50)             null
 ,darts                          varchar(50)             null
 ,american_sports                varchar(50)             null
 ,fishing                        varchar(50)             null
 ,fashion                        varchar(50)             null
 ,music                          varchar(50)             null
 ,betting                        varchar(50)             null
 ,diy                            varchar(50)             null
 ,clubbing                       varchar(50)             null
 ,cooking                        varchar(50)             null
 ,cinema                         varchar(50)             null
 ,bingo                          varchar(50)             null
 ,books                          varchar(50)             null
 ,cars                           varchar(50)             null
 ,gardening                      varchar(50)             null
 ,keep_fit_gym                   varchar(50)             null
 ,theme_parks                    varchar(50)             null
 ,eating_out                     varchar(50)             null
 ,historical_attractions         varchar(50)             null
 ,skiing_snowboarding            varchar(50)             null
 ,no_stated_hobbies_interests    varchar(50)             null
 ,number_of_short_break_holidays varchar(50)             null
 ,number_of_long_break_holidays  varchar(50)             null
 ,cost_of_last_main_holiday      varchar(50)             null
 ,number_of_personal_return_flig varchar(50)             null
 ,number_of_business_return_flig varchar(50)             null
 ,number_of_cars_in_household    varchar(50)             null
 ,cost_of_most_recent_car        varchar(50)             null
 ,make_of_most_recent_car        varchar(50)             null
 ,description_of_most_recent_car varchar(50)             null
 ,mastercard                     varchar(50)             null
 ,visa                           varchar(50)             null
 ,american_express_amex          varchar(50)             null
 ,store_card                     varchar(50)             null
 ,personal_loan                  varchar(50)             null
 ,mortgage                       varchar(50)             null
 ,life_insurance                 varchar(50)             null
 ,health_insurance               varchar(50)             null
 ,savings_or_deposit_accounts    varchar(50)             null
 ,peps                           varchar(50)             null
 ,home_insurance                 varchar(50)             null
 ,isas                           varchar(50)             null
 ,stocks_and_shares              varchar(50)             null
 ,premium_bonds                  varchar(50)             null
 ,private_pension                varchar(50)             null
 ,internet_bank_account          varchar(50)             null
 ,unit_trusts                    varchar(50)             null
 ,no_stated_financial_products   varchar(50)             null
 ,the_sun                        varchar(50)             null
 ,the_mirror                     varchar(50)             null
 ,daily_record                   varchar(50)             null
 ,the_express                    varchar(50)             null
 ,daily_star                     varchar(50)             null
 ,daily_mail                     varchar(50)             null
 ,daily_telegraph                varchar(50)             null
 ,the_guardian_2                 varchar(50)             null
 ,the_independent                varchar(50)             null
 ,the_times                      varchar(50)             null
 ,financial_times                varchar(50)             null
 ,free_daily                     varchar(50)             null
 ,no_stated_daily_newspaper      varchar(50)             null
 ,sunday_times                   varchar(50)             null
 ,sunday_mirror                  varchar(50)             null
 ,news_of_the_world              varchar(50)             null
 ,independent_on_sunday          varchar(50)             null
 ,mail_on_sunday                 varchar(50)             null
 ,the_observer                   varchar(50)             null
 ,sunday_telegraph               varchar(50)             null
 ,sunday_express                 varchar(50)             null
 ,the_people                     varchar(50)             null
 ,daily_star_sunday              varchar(50)             null
 ,sunday_sport                   varchar(50)             null
 ,no_stated_sunday_newspaper     varchar(50)             null
 ,reading_sky_magazine           varchar(50)             null
 ,inside_soap                    varchar(50)             null
 ,radio_times                    varchar(50)             null
 ,the_total_tv_guide             varchar(50)             null
 ,tv_choice                      varchar(50)             null
 ,tv_and_satellite_week          varchar(50)             null
 ,tv_quick                       varchar(50)             null
 ,tv_times                       varchar(50)             null
 ,whats_on_tv                    varchar(50)             null
 ,no_stated_tv_magazine          varchar(50)             null
 ,main_tv_hd_ready               varchar(50)             null
 ,best_describes_main_tv         varchar(50)             null
 ,psp                            varchar(50)             null
 ,satellite_navigation_system    varchar(50)             null
 ,iphone                         varchar(50)             null
 ,nintendo_wii                   varchar(50)             null
 ,mediacentre                    varchar(50)             null
 ,threeg_mobile_phone            varchar(50)             null
 ,hd_dvd_player_blueray_or_hd    varchar(50)             null
 ,laptop_pc_or_apple_mac         varchar(50)             null
 ,desktop_pc_or_apple_mac        varchar(50)             null
 ,no_pc_broadband                varchar(50)             null
 ,aol                            varchar(50)             null
 ,blueyonder                     varchar(50)             null
 ,bt                             varchar(50)             null
 ,bulldog                        varchar(50)             null
 ,onetel                         varchar(50)             null
 ,ntl                            varchar(50)             null
 ,orange_wandoo                  varchar(50)             null
 ,talk_talk                      varchar(50)             null
 ,tiscali                        varchar(50)             null
 ,sky                            varchar(50)             null
 ,other_broadband_provider       varchar(50)             null
 ,access_the_internet            varchar(50)             null
 ,buy_groceries_on_the_internet  varchar(50)             null
 ,download_from_skybroadband     varchar(50)             null
 ,download_from_alternative_webs varchar(50)             null
 ,google                         varchar(50)             null
 ,msn                            varchar(50)             null
 ,yahoo                          varchar(50)             null
 ,ebay                           varchar(50)             null
 ,youtube                        varchar(50)             null
 ,orange_1                       varchar(50)             null
 ,ask                            varchar(50)             null
 ,multimap                       varchar(50)             null
 ,lastminute                     varchar(50)             null
 ,the_sun_1                      varchar(50)             null
 ,amazon                         varchar(50)             null
 ,wikipedia                      varchar(50)             null
 ,friends_reunited               varchar(50)             null
 ,fish4jobs                      varchar(50)             null
 ,sky_sports                     varchar(50)             null
 ,sky_news                       varchar(50)             null
 ,realguide                      varchar(50)             null
 ,expedia                        varchar(50)             null
 ,ivillage                       varchar(50)             null
 ,handbag                        varchar(50)             null
 ,channel4                       varchar(50)             null
 ,the_guardian_1                 varchar(50)             null
 ,apple                          varchar(50)             null
 ,yell                           varchar(50)             null
 ,bbc                            varchar(50)             null
 ,myspace                        varchar(50)             null
 ,skybet                         varchar(50)             null
 ,bebo                           varchar(50)             null
 ,lycos                          varchar(50)             null
 ,not_stated_website             varchar(50)             null
 ,no_mobile_phone                varchar(50)             null
 ,three_network                  varchar(50)             null
 ,o2                             varchar(50)             null
 ,orange_3                       varchar(50)             null
 ,tesco_mobile                   varchar(50)             null
 ,t_mobile                       varchar(50)             null
 ,vodafone                       varchar(50)             null
 ,virgin                         varchar(50)             null
 ,dont_know_network              varchar(50)             null
 ,other_network                  varchar(50)             null
 ,pay_for_use_of_mobile_phone    varchar(50)             null
 ,going_to_the_pub               varchar(50)             null
 ,visiting_casinos               varchar(50)             null
 ,watch_football_in_a_pub_club   varchar(50)             null
 ,how_often_play_the_lottery     varchar(50)             null
 ,sports_betting                 varchar(50)             null
 ,on_line_poker                  varchar(50)             null
 ,casino_style_gaming            varchar(50)             null
 ,no_regular_betting             varchar(50)             null
 ,registered_with_skybet         varchar(50)             null
 ,not_registered                 varchar(50)             null
 ,sports_events_betting          varchar(50)             null
 ,vegas_casino_gaming            varchar(50)             null
 ,dont_know_betting_gaming       varchar(50)             null
 ,classical                      varchar(50)             null
 ,country                        varchar(50)             null
 ,indie                          varchar(50)             null
 ,jazz                           varchar(50)             null
 ,pop                            varchar(50)             null
 ,rock                           varchar(50)             null
 ,decades                        varchar(50)             null
 ,other_music                    varchar(50)             null
 ,spend_on_cds_from_internet     varchar(50)             null
 ,method_of_booking_last_holiday varchar(50)             null
 ,sky_credit_card                varchar(50)             null
 ,motor_insurance                varchar(50)             null
 ,travel_insurance               varchar(50)             null
 ,barclays                       varchar(50)             null
 ,hsbc                           varchar(50)             null
 ,first_direct                   varchar(50)             null
 ,lloyds_tsb                     varchar(50)             null
 ,natwest                        varchar(50)             null
 ,rbs                            varchar(50)             null
 ,cahoot                         varchar(50)             null
 ,any_online_bank_account        varchar(50)             null
 ,any_building_society           varchar(50)             null
 ,other_bank                     varchar(50)             null
 ,womens_general_interest        varchar(50)             null
 ,womens_weeklies                varchar(50)             null
 ,womens_lifestyle               varchar(50)             null
 ,home                           varchar(50)             null
 ,news_and_current_affairs       varchar(50)             null
 ,sport                          varchar(50)             null
 ,pc_computing                   varchar(50)             null
 ,mens_weeklies                  varchar(50)             null
 ,mens_lifestyle                 varchar(50)             null
 ,celebrity_and_gossip           varchar(50)             null
 ,music_1                        varchar(50)             null
 ,no_stated_magazine             varchar(50)             null
 ,burger_king                    varchar(50)             null
 ,mcdonalds                      varchar(50)             null
 ,kfc                            varchar(50)             null
 ,pizza_hut                      varchar(50)             null
 ,dominos                        varchar(50)             null
 ,pizza_express                  varchar(50)             null
 ,subway                         varchar(50)             null
 ,other_restaurant               varchar(50)             null
 ,no_stated_restaurant           varchar(50)             null
 ,where_main_grocery_shopped     varchar(50)             null
 ,spend_on_skincare              varchar(50)             null
 ,how_important_looking_good     varchar(50)             null
 ,moisturisers_supermarket       varchar(50)             null
 ,cleanser_supermarket           varchar(50)             null
 ,body_lotion_supermarket        varchar(50)             null
 ,self_tan_supermarket           varchar(50)             null
 ,hair_colourants_supermarket    varchar(50)             null
 ,styling_products_supermarket   varchar(50)             null
 ,shampoo_conditioner_supermket  varchar(50)             null
 ,cosmetics_supermarket          varchar(50)             null
 ,suncream_supermarket           varchar(50)             null
 ,no_stated_beauty_products_smkt varchar(50)             null
 ,moisturisers_chemist           varchar(50)             null
 ,cleanser_chemist               varchar(50)             null
 ,body_lotion_chemist            varchar(50)             null
 ,self_tan_chemist               varchar(50)             null
 ,hair_colourants_chemist        varchar(50)             null
 ,styling_products_chemist       varchar(50)             null
 ,shampoo_conditioner_chemist    varchar(50)             null
 ,cosmetics_chemist              varchar(50)             null
 ,suncream_chemist               varchar(50)             null
 ,no_stated_beauty_products_chm  varchar(50)             null
 ,what_type_of_property          varchar(50)             null
 ,bed                            varchar(50)             null
 ,sofa_armchair                  varchar(50)             null
 ,dining_room_suite              varchar(50)             null
 ,cooker                         varchar(50)             null
 ,microwave_oven                 varchar(50)             null
 ,fridge_fridge_freezer          varchar(50)             null
 ,washing_machine_tumble_drier   varchar(50)             null
 ,dishwasher                     varchar(50)             null
 ,vacuum_cleaner                 varchar(50)             null
 ,no_intending_purchases         varchar(50)             null
 ,which_ethnic_cultural_backgrd  varchar(50)             null
 ,hd_active_status               varchar(50)             null
 ,hd_active_status_date          varchar(50)             null
 ,number_of_sky_movies_downloads varchar(50)             null
 ,number_of_sky_sports_downloads varchar(50)             null
 ,no_of_downloads_via_mobile_eve varchar(50)             null
 ,no_of_downloads_via_pc_ever    varchar(50)             null
 ,no_of_downloads_via_pc_30_day  varchar(50)             null
 ,three_broadband                varchar(50)             null
 ,o2_1                           varchar(50)             null
 ,pipex                          varchar(50)             null
 ,post_office                    varchar(50)             null
 ,t_mobile_1                     varchar(50)             null
 ,virgin_media                   varchar(50)             null
 ,vodafone_1                     varchar(50)             null
 ,number_of_visits_to_cinema     varchar(50)             null
 ,frequency_buying_dvds          varchar(50)             null
 ,newspaper_website_regularly    varchar(50)             null
 ,news_website_regularly         varchar(50)             null
 ,financial_website_regularly    varchar(50)             null
 ,job_seeking_website_regularly  varchar(50)             null
 ,property_website_regularly     varchar(50)             null
 ,directories_website_regularly  varchar(50)             null
 ,travel_website_regularly       varchar(50)             null
 ,weather_website_regularly      varchar(50)             null
 ,music_website_regularly        varchar(50)             null
 ,cinema_website_regularly       varchar(50)             null
 ,tv_website_regularly           varchar(50)             null
 ,football_website_regularly     varchar(50)             null
 ,other_sport_website_regularly  varchar(50)             null
 ,motoring_website_regularly     varchar(50)             null
 ,social_networking_website_regu varchar(50)             null
 ,video_website_regularly        varchar(50)             null
 ,orange_2                       varchar(50)             null
 ,o2_2                           varchar(50)             null
 ,vodafone_2                     varchar(50)             null
 ,carphone_warehouse             varchar(50)             null
 ,t_mobile_2                     varchar(50)             null
 ,phones_4_u                     varchar(50)             null
 ,tesco                          varchar(50)             null
 ,currys                         varchar(50)             null
 ,dixons                         varchar(50)             null
 ,argos                          varchar(50)             null
 ,comet                          varchar(50)             null
 ,internet_specialist            varchar(50)             null
 ,other_specialist_phone_shop    varchar(50)             null
 ,other_supermarket_hypermarket  varchar(50)             null
 ,other_electrical_retailer      varchar(50)             null
 ,company_mobile                 varchar(100)            null
 ,bought_for_me                  varchar(50)             null
 ,received_as_gift               varchar(50)             null
 ,other_outlet                   varchar(50)             null
 ,how_pay_for_mobile             varchar(50)             null
 ,in_store_gambling              varchar(50)             null
 ,on_line_gambling               varchar(50)             null
 ,interactive_gambling           varchar(50)             null
 ,telephone_gambling             varchar(50)             null
 ,other_gambling_method          varchar(50)             null
 ,no_other_gambling_method       varchar(50)             null
 ,beach_resort_holiday           varchar(50)             null
 ,second_set_top_box             varchar(50)             null
 ,broadband_subscription         varchar(50)             null
 ,broadband_subscription_date    varchar(50)             null
 ,anytime_pc                     varchar(50)             null
 ,anytime_pc_date                varchar(50)             null
 ,anytime_mobile                 varchar(50)             null
 ,anytime_mobile_date            varchar(50)             null
 ,lakes_and_mountains            varchar(50)             null
 ,no_holiday                     varchar(50)             null
 ,abbey                          varchar(50)             null
 ,alliance_and_leicester         varchar(50)             null
 ,bank_of_scotland               varchar(50)             null
 ,co_op_bank                     varchar(50)             null
 ,halifax                        varchar(50)             null
 ,ing_direct                     varchar(50)             null
 ,nationwide                     varchar(50)             null
 ,northern_rock                  varchar(50)             null
 ,virgin_1                       varchar(50)             null
 ,woolwich                       varchar(50)             null
 ,yorkshire_bank                 varchar(50)             null
 ,any_supermarkets_own_bank      varchar(50)             null
 ,other_bank_building_society    varchar(50)             null
 ,nandos                         varchar(50)             null
 ,groceries_high_st              varchar(50)             null
 ,groceries_on_line              varchar(50)             null
 ,alcohol_high_st                varchar(50)             null
 ,alcohol_on_line                varchar(50)             null
 ,womens_clothes_high_st         varchar(50)             null
 ,womens_clothes_on_line         varchar(50)             null
 ,mens_clothes_high_st           varchar(50)             null
 ,mens_clothes_on_line           varchar(50)             null
 ,dvds_high_st                   varchar(50)             null
 ,dvds_on_line                   varchar(50)             null
 ,cds_high_st                    varchar(50)             null
 ,cds_on_line                    varchar(50)             null
 ,computer_games_high_st         varchar(50)             null
 ,computer_games_on_line         varchar(50)             null
 ,consumer_technology_high_st    varchar(50)             null
 ,consumer_technology_on_line    varchar(50)             null
 ,kitchen_products_high_st       varchar(50)             null
 ,kitchen_products_on_line       varchar(50)             null
 ,home_furnishings_high_st       varchar(50)             null
 ,home_furnishings_on_line       varchar(50)             null
 ,diy_products_high_st           varchar(50)             null
 ,diy_products_on_line           varchar(50)             null
 ,gardening_products_high_st     varchar(50)             null
 ,gardening_products_on_line     varchar(50)             null
 ,sports_equipment_high_st       varchar(50)             null
 ,sports_equipment_on_line       varchar(50)             null
 ,skincare_and_cosmetics_high_st varchar(50)             null
 ,skincare_and_cosmetics_on_line varchar(50)             null
 ,lager_in_home                  varchar(50)             null
 ,lager_out_of_home              varchar(50)             null
 ,beer_in_home                   varchar(50)             null
 ,beer_out_of_home               varchar(50)             null
 ,white_wine_in_home             varchar(50)             null
 ,white_wine_out_of_home         varchar(50)             null
 ,red_wine_in_home               varchar(50)             null
 ,red_wine_out_of_home           varchar(50)             null
 ,champagne_in_home              varchar(50)             null
 ,champagne_out_of_home          varchar(50)             null
 ,vodka_in_home                  varchar(50)             null
 ,vodka_out_of_home              varchar(50)             null
 ,gin_in_home                    varchar(50)             null
 ,gin_out_of_home                varchar(50)             null
 ,rum_in_home                    varchar(50)             null
 ,rum_out_of_home                varchar(50)             null
 ,whiskey_in_home                varchar(50)             null
 ,whiskey_out_of_home            varchar(50)             null
 ,other_spirit_in_home           varchar(50)             null
 ,other_spirit_out_of_home       varchar(50)             null
 ,alcohol_flavored_beverage_in   varchar(50)             null
 ,alcohol_flavored_beverage_out  varchar(50)             null
 ,finish_school                  varchar(50)             null
 ,start_university               varchar(50)             null
 ,start_first_job                varchar(50)             null
 ,change_job                     varchar(50)             null
 ,first_time_buyer               varchar(50)             null
 ,move_house_or_flat             varchar(50)             null
 ,renovate_home                  varchar(50)             null
 ,birth_of_child                 varchar(50)             null
 ,child_leaves_home              varchar(50)             null
 ,get_engaged                    varchar(50)             null
 ,get_married                    varchar(50)             null
 ,buy_a_car                      varchar(50)             null
 ,cigarettes_per_day             varchar(50)             null
 ,ever_given_up_smoking          varchar(50)             null
 ,category_of_cigarette_smoker   varchar(50)             null
 ,run_own_business               varchar(50)             null
 ,how_many_uk_employees          varchar(50)             null
 ,newspaper_website_occasionally varchar(50)             null
 ,news_website_occasionally      varchar(50)             null
 ,financial_website_occasionally varchar(50)             null
 ,job_seeking_website_occasional varchar(50)             null
 ,property_website_occasionally  varchar(50)             null
 ,directories_website_occasional varchar(50)             null
 ,travel_website_occasionally    varchar(50)             null
 ,weather_website_occasionally   varchar(50)             null
 ,music_website_occasionally     varchar(50)             null
 ,pcprizmcode                    varchar(50)             null
 ,premship                       varchar(50)             null
 ,tvregion                       varchar(50)             null
 ,cablepresence                  varchar(50)             null
 ,revcat                         varchar(50)             null
 ,stb_manufacturer               varchar(50)             null
 ,householdprizm                 varchar(50)             null
 ,current_high_tier              varchar(50)             null
 ,current_middle_tier            varchar(50)             null
 ,current_lower_tier             varchar(50)             null
 ,hhaffluence                    varchar(50)             null
 ,pcaffluence                    varchar(50)             null
 ,pclifestage                    varchar(50)             null
 ,current_lower_tier_2           varchar(50)             null
 ,previous_lower_tier_2          varchar(50)             null
 ,disney                         varchar(50)             null
 ,music_choice                   varchar(50)             null
 ,mutv                           varchar(50)             null
 ,film_four                      varchar(50)             null
 ,atrsworld                      varchar(50)             null
 ,star_tv                        varchar(50)             null
 ,chelsea_tv                     varchar(50)             null
 ,sky_plus                       varchar(50)             null
 ,date_aquired_sky_plus          varchar(50)             null
 ,previous_high_tier             varchar(50)             null
 ,previous_middle_tier           varchar(50)             null
 ,previous_lower_tier            varchar(50)             null
 ,second_set_top_box_date        varchar(50)             null
 ,subscriber_churn_count         varchar(50)             null
 ,contract_duration              varchar(50)             null
 ,upgrade_downgrade_crossgrade   varchar(50)             null
 ,upgrade_downgrade_date         varchar(50)             null
 ,customer_active_date           varchar(50)             null
 ,date_of_last_statement_amend   varchar(50)             null
 ,effective_date_of_detail_pack  varchar(50)             null
 ,customer_bskyb_status          varchar(50)             null
 ,routetomarket                  varchar(50)             null
 ,paymethod                      varchar(50)             null
 ,cancelcode                     varchar(50)             null
 ,cardstatus                     varchar(50)             null
 ,cancelcode_groups              varchar(50)             null
 ,sky_sports_extra               varchar(50)             null
 ,stb_model_used_for_970         varchar(50)             null
 ,hd_status                      varchar(50)             null
 ,hd_status_date                 varchar(50)             null
 ,account_number                 varchar(20)             null
 ,src_system_id                  varchar(50)             null
 ,cb_key_household               bigint                  null
 ,cb_key_family                  bigint                  null
 ,cb_key_individual              bigint                  null
 ,cb_key_db_person               bigint                  null
);


INSERT INTO vespa_analysts.VESPA_KANTAR_SURVEY_DATA_2011_02
   LOCATION 'DCSLOPSKPRD10_olive_prod.sk_prodreg' 'SELECT * FROM sk_prodreg.VESPA_KANTAR_SURVEY_DATA';
commit;

grant all on vespa_analysts.VESPA_KANTAR_SURVEY_DATA_2011_02 to public;

create HG index CB_KEY_DB_PERSON_HG on vespa_analysts.VESPA_KANTAR_SURVEY_DATA_2011_02 (cb_key_db_person);
create HG index CB_KEY_FAMILY_HG on vespa_analysts.VESPA_KANTAR_SURVEY_DATA_2011_02 (cb_key_family);
create HG index CB_KEY_HOUSEHOLD_HG on vespa_analysts.VESPA_KANTAR_SURVEY_DATA_2011_02 (cb_key_household);
create HG index CB_KEY_INDIVIDUAL_HG on vespa_analysts.VESPA_KANTAR_SURVEY_DATA_2011_02 (cb_key_individual);
;
commit;
---Log Out from vespa_analysts then log in as dbarnett---
--drop table dbarnett.VESPA_KANTAR_SURVEY_DATA_2011_02;
select * into dbarnett.VESPA_KANTAR_SURVEY_DATA_2011_02 from vespa_analysts.VESPA_KANTAR_SURVEY_DATA_2011_02; commit;
grant select on dbarnett.VESPA_KANTAR_SURVEY_DATA_2011_02 to public;  commit;
--Log back in as vespa_analysts to Drop vespa_analysts version of table---
drop table vespa_analysts.VESPA_KANTAR_SURVEY_DATA_2011_02; commit;

----Put the Relevant details in temp table---
--drop table #kantar_survey_key_data;
select hhno                         
 ,individ                       
 ,customerid                    
 ,estabhomeno                   
 ,cardholderid                  
 ,wphouseholdnumber             
 ,reportedondatadate            
 ,sex                           
 ,cast (age as integer) as age_value
 ,hw_status                     
 ,terminal_age_of_education     
 ,employment_status             
,social_class                  
 ,lifecycle                     
,account_number                
 ,src_system_id                 
 ,cb_key_household              
 ,cb_key_family                 
 ,cb_key_individual             
 ,cb_key_db_person   
into #kantar_survey_key_data
from dbarnett.VESPA_KANTAR_SURVEY_DATA_2011_02
;
commit;           

----Import More recent survey data (needs to be deduped)------
--drop table dbarnett.VESPA_KANTAR_SURVEY_DATA_2012_07_HH_SUMMARY;
create table dbarnett.VESPA_KANTAR_SURVEY_DATA_2012_07_HH_SUMMARY

(Household	integer
,Total_in_hh	integer
,Male_18_plus	integer
,Female_18_plus	integer
,Under18_but_aged_4_plus	integer
,Any_children_under_4	integer
,Total_children_under_4	integer
,Male_18_25	integer
,Male_26_35	integer
,Male_36_45	integer
,Male_46_55	integer
,Male_56_65	integer
,Male_66_plus	integer
,Male_age_unknown	integer
,Female_18_25	integer
,Female_26_35	integer
,Female_36_45	integer
,Female_46_55	integer
,Female_56_65	integer
,Female_66_plus	integer
,Female_age_unknown integer
);
commit;

input into dbarnett.VESPA_KANTAR_SURVEY_DATA_2012_07_HH_SUMMARY from 'C:\Users\barnetd\Documents\Project 078 - Vespa Household to Individual\Infosys Survey Data 20120701 to 20120707 HH Summary.csv' format ascii;
commit;
grant select on dbarnett.VESPA_KANTAR_SURVEY_DATA_2012_07_HH_SUMMARY to public;

---Add on Account_number from old survey data---

---Where there is no match is where there are no details for that Household on the Feb 2011 Kantar Survey----

alter table dbarnett.VESPA_KANTAR_SURVEY_DATA_2012_07_HH_SUMMARY add account_number varchar(20);
update dbarnett.VESPA_KANTAR_SURVEY_DATA_2012_07_HH_SUMMARY
set account_number=b.account_number
from dbarnett.VESPA_KANTAR_SURVEY_DATA_2012_07_HH_SUMMARY as a
left outer join dbarnett.VESPA_KANTAR_SURVEY_DATA_2011_02 as b
on a.Household = cast(b.hhno as integer) 
;
grant all on dbarnett.VESPA_KANTAR_SURVEY_DATA_2012_07_HH_SUMMARY to public;

---Add Current Household key (rather than using HH key from Feb 2011 Survey
---Only Apply HH Key to PAF valid Households
--alter table dbarnett.VESPA_KANTAR_SURVEY_DATA_2012_07_HH_SUMMARY delete cb_key_household;

alter table dbarnett.VESPA_KANTAR_SURVEY_DATA_2012_07_HH_SUMMARY add cb_key_household bigint;
update dbarnett.VESPA_KANTAR_SURVEY_DATA_2012_07_HH_SUMMARY
set cb_key_household=b.cb_key_household
from dbarnett.VESPA_KANTAR_SURVEY_DATA_2012_07_HH_SUMMARY as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number=b.account_number
where b.cb_address_status = '1' and b.cb_address_dps is not null 
;
commit;

---Create Summary of One record per Household from Experian consumerview----

--drop table #consumerview_data_one_record_per_hh;
select cb_key_household
,sum(case when p_gender = '0' then 1 else 0 end) as males
,sum(case when p_gender = '1' then 1 else 0 end) as females
,sum(case when p_gender = 'U' then 1 else 0 end) as unknown_gender
,sum(case when p_gender = '0' and person_age = '0'  then 1 else 0 end) as males_aged_18_25
,sum(case when p_gender = '0' and person_age = '1'  then 1 else 0 end) as males_aged_26_35
,sum(case when p_gender = '0' and person_age = '2'  then 1 else 0 end) as males_aged_36_45
,sum(case when p_gender = '0' and person_age = '3'  then 1 else 0 end) as males_aged_46_55
,sum(case when p_gender = '0' and person_age = '4'  then 1 else 0 end) as males_aged_56_65
,sum(case when p_gender = '0' and person_age = '5'  then 1 else 0 end) as males_aged_66_plus
,sum(case when p_gender = '0' and person_age = 'U'  then 1 else 0 end) as males_aged_unk

,sum(case when p_gender = '1' and person_age = '0'  then 1 else 0 end) as females_aged_18_25
,sum(case when p_gender = '1' and person_age = '1'  then 1 else 0 end) as females_aged_26_35
,sum(case when p_gender = '1' and person_age = '2'  then 1 else 0 end) as females_aged_36_45
,sum(case when p_gender = '1' and person_age = '3'  then 1 else 0 end) as females_aged_46_55
,sum(case when p_gender = '1' and person_age = '4'  then 1 else 0 end) as females_aged_56_65
,sum(case when p_gender = '1' and person_age = '5'  then 1 else 0 end) as females_aged_66_plus
,sum(case when p_gender = '1' and person_age = 'U'  then 1 else 0 end) as females_aged_unk

,sum(case when p_gender = 'U' and person_age = '0'  then 1 else 0 end) as unknown_gender_aged_18_25
,sum(case when p_gender = 'U' and person_age = '1'  then 1 else 0 end) as unknown_gender_aged_26_35
,sum(case when p_gender = 'U' and person_age = '2'  then 1 else 0 end) as unknown_gender_aged_36_45
,sum(case when p_gender = 'U' and person_age = '3'  then 1 else 0 end) as unknown_gender_aged_46_55
,sum(case when p_gender = 'U' and person_age = '4'  then 1 else 0 end) as unknown_gender_aged_56_65
,sum(case when p_gender = 'U' and person_age = '5'  then 1 else 0 end) as unknown_gender_aged_66_plus
,sum(case when p_gender = 'U' and person_age = 'U'  then 1 else 0 end) as unknown_gender_aged_unk

,max(case when family_lifestage in ('02','03','06','07','10') then 1 else 0 end) as presence_of_children

into #consumerview_data_one_record_per_hh
from sk_prod.experian_consumerview as a
where cb_address_status = '1' and cb_address_dps is not null 
group by cb_key_household
;
commit;
create hg index idx1 on #consumerview_data_one_record_per_hh (cb_key_household);
commit;

---Add Consumerview details on to Kantar HH Survey Summary----

--drop table  dbarnett.project078_kantar_survey_and_consumerview_hh_summary;

select a.*
,b.males as consumerview_males
,b.females as consumerview_females 
,b.unknown_gender as consumerview_unknown_gender
,b.males_aged_18_25 as consumerview_males_aged_18_25
,b.males_aged_26_35 as consumerview_males_aged_26_35
,b.males_aged_36_45 as consumerview_males_aged_36_45
,b.males_aged_46_55 as consumerview_males_aged_46_55
,b.males_aged_56_65 as consumerview_males_aged_56_65
,b.males_aged_66_plus as consumerview_males_aged_66_plus
,b.males_aged_unk as consumerview_males_aged_unk

,b.females_aged_18_25 as consumerview_females_aged_18_25
,b.females_aged_26_35 as consumerview_females_aged_26_35
,b.females_aged_36_45 as consumerview_females_aged_36_45
,b.females_aged_46_55 as consumerview_females_aged_46_55
,b.females_aged_56_65 as consumerview_females_aged_56_65
,b.females_aged_66_plus as consumerview_females_aged_66_plus
,b.females_aged_unk as consumerview_females_aged_unk

,b.unknown_gender_aged_18_25 as consumerview_unknown_gender_aged_18_25
,b.unknown_gender_aged_26_35 as consumerview_unknown_gender_aged_26_35
,b.unknown_gender_aged_36_45 as consumerview_unknown_gender_aged_36_45
,b.unknown_gender_aged_46_55 as consumerview_unknown_gender_aged_46_55
,b.unknown_gender_aged_56_65 as consumerview_unknown_gender_aged_56_65
,b.unknown_gender_aged_66_plus as consumerview_unknown_gender_aged_66_plus
,b.unknown_gender_aged_unk as consumerview_unknown_gender_aged_unk
,b.presence_of_children as consumerview_presence_of_children
into dbarnett.project078_kantar_survey_and_consumerview_hh_summary
from dbarnett.VESPA_KANTAR_SURVEY_DATA_2012_07_HH_SUMMARY as a
left outer join #consumerview_data_one_record_per_hh as b
on a.cb_key_household=b.cb_key_household;
commit;

grant all on dbarnett.project078_kantar_survey_and_consumerview_hh_summary to public;

--select top 500 * from dbarnett.project078_kantar_survey_and_consumerview_hh_summary;

--select count(*) from dbarnett.project078_kantar_survey_and_consumerview_hh_summary where account_number is null

select male_18_plus+female_18_plus as kantar_18_plus_in_hh
,consumerview_males+consumerview_females+consumerview_unknown_gender as consumerview_18_plus_in_hh
,count(*) as accounts
from dbarnett.project078_kantar_survey_and_consumerview_hh_summary
where account_number is not null    ---i.e.,. we have a match to the old survey data so can get HH Key data
and consumerview_males is not null  ---has a hhkey match to consumerview data
group by  kantar_18_plus_in_hh
,consumerview_18_plus_in_hh
order by kantar_18_plus_in_hh
,consumerview_18_plus_in_hh
;

--select * from dbarnett.project078_kantar_survey_and_consumerview_hh_summary where male_18_plus+female_18_plus=23

commit;



