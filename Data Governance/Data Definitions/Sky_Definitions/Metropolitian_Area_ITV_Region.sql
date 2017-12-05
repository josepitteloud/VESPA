/*###############################################################################
# Created on:   17/12/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for defining the Metropolitian ITV Region
#		area for a given household(s)
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 17/12/2012  TKD   v01 - initial version
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Process									    #####
-- ##############################################################################################################

--example below based on adsmart feed


select account_number
        ,cb_key_household
        ,barb_desc_itv
        ,trim(upper(cb_address_town))           as pc_town
        ,substr(cb_address_postcode,1,2)        as pc_area
into pc_area_table
from AdSmart;
commit;


--------------------
select *
        ,(case when substr(pc_area,2,1) in ('1','2','3','4','5','6','7','8','9','0')
               then substr(pc_area,1,1) else pc_area end) as pc_area2
into pc_area_table2
from pc_area_table;
commit;

select *,(case when pc_area2= 'AB' then 'Aberdeen'
                when pc_area2= 'AL' then 'St. Albans'
                when pc_area2= 'B' then 'Birmingham'
                when pc_area2= 'BA' then 'Bath'
                when pc_area2= 'BB' then 'Blackburn'
                when pc_area2= 'BD' then 'Bradford'
                when pc_area2= 'BH' then 'Bournemouth'
                when pc_area2= 'BL' then 'Bolton'
                when pc_area2= 'BN' then 'Brighton'
                when pc_area2= 'BR' then 'Bromley'
                when pc_area2= 'BS' then 'Bristol'
                when pc_area2= 'BT' then 'Belfast'
                when pc_area2= 'CA' then 'Carlisle'
                when pc_area2= 'CB' then 'Cambridge'
                when pc_area2= 'CF' then 'Cardiff'
                when pc_area2= 'CH' then 'Chester'
                when pc_area2= 'CM' then 'Chelmsford'
                when pc_area2= 'CO' then 'Colchester'
                when pc_area2= 'CR' then 'Croydon'
                when pc_area2= 'CT' then 'Canterbury'
                when pc_area2= 'CV' then 'Coventry'
                when pc_area2= 'CW' then 'Crewe'
                when pc_area2= 'DA' then 'Dartford'
                when pc_area2= 'DD' then 'Dundee'
                when pc_area2= 'DE' then 'Derby'
                when pc_area2= 'DG' then 'Dumfries'
                when pc_area2= 'DH' then 'Durham'
                when pc_area2= 'DL' then 'Darlington'
                when pc_area2= 'DN' then 'Doncaster'
                when pc_area2= 'DT' then 'Dorchester'
                when pc_area2= 'DY' then 'Dudley'
                when pc_area2= 'E' then 'London East'
                when pc_area2= 'EC' then 'London East Central'
                when pc_area2= 'EH' then 'Edinburgh'
                when pc_area2= 'EN' then 'Enfield'
                when pc_area2= 'EX' then 'Exeter'
                when pc_area2= 'FK' then 'Falkirk'
                when pc_area2= 'FY' then 'Fylde (Blackpool)'
                when pc_area2= 'G' then 'Glasgow'
                when pc_area2= 'GL' then 'Gloucester'
                when pc_area2= 'GU' then 'Guildford'
                when pc_area2= 'GY' then 'Guernsey & Alderney'
                when pc_area2= 'HA' then 'Harrow'
                when pc_area2= 'HD' then 'Huddersfield'
                when pc_area2= 'HG' then 'Harrogate'
                when pc_area2= 'HP' then 'Hemel Hempstead'
                when pc_area2= 'HR' then 'Hereford'
                when pc_area2= 'HS' then 'HEBRIDES'
                when pc_area2= 'HU' then 'Hull'
                when pc_area2= 'HX' then 'Halifax'
                when pc_area2= 'IG' then 'Ilford'
                when pc_area2= 'IM' then 'Isle of Man'
                when pc_area2= 'IP' then 'Ipswich'
                when pc_area2= 'IV' then 'Inverness'
                when pc_area2= 'JE' then 'Jersey'
                when pc_area2= 'KA' then 'Kilmarnock'
                when pc_area2= 'KT' then 'Kingston Upon Thames'
                when pc_area2= 'KW' then 'Kirkwall'
                when pc_area2= 'KY' then 'Kirkcaldy'
                when pc_area2= 'L' then 'Liverpool'
                when pc_area2= 'LA' then 'Lancaster'
                when pc_area2= 'LD' then 'Llandrindod Wells'
                when pc_area2= 'LE' then 'Leicester'
                when pc_area2= 'LL' then 'Llandudno'
                when pc_area2= 'LN' then 'Lincoln'
                when pc_area2= 'LS' then 'Leeds'
                when pc_area2= 'LU' then 'Luton'
                when pc_area2= 'M'  then 'Manchester'
                when pc_area2= 'ME' then 'Medway (Rochester)'
                when pc_area2= 'MK' then 'Milton Keynes'
                when pc_area2= 'ML' then 'Motherwell'
                when pc_area2= 'N'  then 'London North'
                when pc_area2= 'NE' then 'Newcastle on Tyne'
                when pc_area2= 'NG' then 'Nottingham'
                when pc_area2= 'NN' then 'Northampton'
                when pc_area2= 'NP' then 'Newport'
                when pc_area2= 'NR' then 'Norwich'
                when pc_area2= 'NW' then 'London North West'
                when pc_area2= 'OL' then 'Oldham'
                when pc_area2= 'OX' then 'Oxford'
                when pc_area2= 'PA' then 'Paisley'
                when pc_area2= 'PE' then 'Peterborough'
                when pc_area2= 'PH' then 'Perth'
                when pc_area2= 'PL' then 'Plymouth'
                when pc_area2= 'PO' then 'Portsmouth'
                when pc_area2= 'PR' then 'Preston'
                when pc_area2= 'RG' then 'Reading'
                when pc_area2= 'RH' then 'Redhill'
                when pc_area2= 'RM' then 'Romford'
                when pc_area2= 'S' then 'Sheffield'
                when pc_area2= 'SA' then 'Swansea'
                when pc_area2= 'SE' then 'London South East'
                when pc_area2= 'SG' then 'Stevenage'
                when pc_area2= 'SK' then 'Stockport'
                when pc_area2= 'SL' then 'Slough'
                when pc_area2= 'SM' then 'Sutton'
                when pc_area2= 'SN' then 'Swindon'
                when pc_area2= 'SO' then 'Southampton'
                when pc_area2= 'SP' then 'Salisbury'
                when pc_area2= 'SR' then 'Sunderland'
                when pc_area2= 'SS' then 'Southend on Sea'
                when pc_area2= 'ST' then 'Stoke On Trent'
                when pc_area2= 'SW' then 'London South West'
                when pc_area2= 'SY' then 'Shrewsbury'
                when pc_area2= 'TA' then 'Taunton'
                when pc_area2= 'TD' then 'Berwick upon Tweed'
                when pc_area2= 'TF' then 'Telford'
                when pc_area2= 'TN' then 'Tunbridge Wells'
                else null
                end) as pc_area3
into temp
from pc_area_table2;
commit;

-- Because of overflow error msg, repeat lookup for the remaining postcode areas


select *, (case when pc_area2= 'TQ' then 'Torquay'
                when pc_area2= 'TR' then 'Truro'
                when pc_area2= 'TS' then 'Teesside (Middlesbrough)'
                when pc_area2= 'TW' then 'Twickenham'
                when pc_area2= 'UB' then 'Uxbridge'
                when pc_area2= 'W' then 'London West'
                when pc_area2= 'WA' then 'Warrington'
                when pc_area2= 'WC' then 'London West Central'
                when pc_area2= 'WD' then 'Watford'
                when pc_area2= 'WF' then 'Wakefield'
                when pc_area2= 'WN' then 'Wigan'
                when pc_area2= 'WR' then 'Worcester'
                when pc_area2= 'WS' then 'Walsall'
                when pc_area2= 'WV' then 'Wolverhampton'
                when pc_area2= 'YO' then 'York'
                when pc_area2= 'ZE' then 'Lerwick'
                else null
                end) as pc_area4,
                trim(upper(coalesce(pc_area3, pc_area4))) as pc_area_desc
into pc_area_table3
from temp;
commit;

drop table temp;


-- group postcode areas into metropolitan areas from the lookup table

select *,(case when pc_town is null then pc_area_desc else pc_town end) as postcode_town,
         (case when postcode_town=  'BIRMINGHAM' then 'Birmingham metropolitan area'
            when postcode_town=  'WOLVERHAMPTON' then 'Birmingham metropolitan area'
            when postcode_town=  'COVENTRY' then 'Birmingham metropolitan area'
            when postcode_town=  'NUNEATON' then 'Birmingham metropolitan area'
            when postcode_town=  'WARWICK' then 'Birmingham metropolitan area'
            when postcode_town=  'LEAMINGTON' then 'Birmingham metropolitan area'
            when postcode_town=  'REDDITCH' then 'Birmingham metropolitan area'
            when postcode_town=  'BROMSGROVE' then 'Birmingham metropolitan area'
            when postcode_town=  'TAMWORTH' then 'Birmingham metropolitan area'

            when postcode_town=  'MANCHESTER' then 'Manchester metropolitan area'
            when postcode_town=  'MACCLESFIELD' then 'Manchester metropolitan area'

            when postcode_town=  'LEEDS' then 'Leeds-Bradford metropolitan area'
            when postcode_town=  'BRADFORD' then 'Leeds-Bradford metropolitan area'
            when postcode_town=  'HUDDERSFIELD' then 'Leeds-Bradford metropolitan area'
            when postcode_town=  'HALIFAX' then 'Leeds-Bradford metropolitan area'
            when postcode_town=  'QUEENSBURY' then 'Leeds-Bradford metropolitan area'
            when postcode_town=  'WAKEFIELD' then 'Leeds-Bradford metropolitan area'
            when postcode_town=  'CASTLEFORD' then 'Leeds-Bradford metropolitan area'
            when postcode_town=  'PONTEFRACT' then 'Leeds-Bradford metropolitan area'
            when postcode_town=  'HARROGATE' then 'Leeds-Bradford metropolitan area'
            when postcode_town=  'DEWSBURY' then 'Leeds-Bradford metropolitan area'

            when postcode_town=  'LIVERPOOL' then 'Liverpool/Birkenhead metropolitan area'
            when postcode_town=  'BIRKENHEAD' then 'Liverpool/Birkenhead metropolitan area'
            when postcode_town=  'WIGAN' then 'Liverpool/Birkenhead metropolitan area'
            when postcode_town=  'ASHTON' then 'Liverpool/Birkenhead metropolitan area'
            when postcode_town=  'WARRINGTON' then 'Liverpool/Birkenhead metropolitan area'
            when postcode_town=  'WIDNES' then 'Liverpool/Birkenhead metropolitan area'
            when postcode_town=  'RUNCORN' then 'Liverpool/Birkenhead metropolitan area'
            when postcode_town=  'CHESTER' then 'Liverpool/Birkenhead metropolitan area'
            when postcode_town=  'SOUTHPORT' then 'Liverpool/Birkenhead metropolitan area'
            when postcode_town=  'ELLESMERE PORT' then 'Liverpool/Birkenhead metropolitan area'
            when postcode_town=  'ORMSKIRK' then 'Liverpool/Birkenhead metropolitan area'
            when postcode_town=  'SKELMERSDALE' then 'Liverpool/Birkenhead metropolitan area'

            when postcode_town=  'NEWCASTLE' then 'Newcastle-Sunderland metropolitan area'
            when postcode_town=  'SUNDERLAND' then 'Newcastle-Sunderland metropolitan area'
            when postcode_town=  'BLYTH' then 'Newcastle-Sunderland metropolitan area'
            when postcode_town=  'CRAMLINGTON' then 'Newcastle-Sunderland metropolitan area'
            when postcode_town=  'PETERLEE' then 'Newcastle-Sunderland metropolitan area'
            when postcode_town=  'ASHINGTON' then 'Newcastle-Sunderland metropolitan area'
            when postcode_town=  'SEAHAM' then 'Newcastle-Sunderland metropolitan area'
            when postcode_town=  'CHESTER-LE-STREET' then 'Newcastle-Sunderland metropolitan area'

            when postcode_town=  'SHEFFIELD' then 'Sheffield metropolitan area'
            when postcode_town=  'ROTHERHAM' then 'Sheffield metropolitan area'
            when postcode_town=  'DONCASTER' then 'Sheffield metropolitan area'
            when postcode_town=  'DARFIELD' then 'Sheffield metropolitan area'
            when postcode_town=  'CHESTERFIELD' then 'Sheffield metropolitan area'
            when postcode_town=  'BARNSLEY' then 'Sheffield metropolitan area'

            when postcode_town=  'PORTSMOUTH' then 'Portsmouth/Southampton metropolitan area'
            when postcode_town=  'SOUTHAMPTON' then 'Portsmouth/Southampton metropolitan area'
            when postcode_town=  'BOGNOR REGIS' then 'Portsmouth/Southampton metropolitan area'
            when postcode_town=  'SALISBURY' then 'Portsmouth/Southampton metropolitan area'
            when postcode_town=  'WINCHESTER' then 'Portsmouth/Southampton metropolitan area'
            when postcode_town=  'ANDOVER' then 'Portsmouth/Southampton metropolitan area'

            when postcode_town=  'NOTTINGHAM' then 'Nottingham-Derby metropolitan area'
            when postcode_town=  'DERBY' then 'Nottingham-Derby metropolitan area'
            when postcode_town=  'LLKESTON' then 'Nottingham-Derby metropolitan area'
            when postcode_town=  'ALFRETON' then 'Nottingham-Derby metropolitan area'

            when postcode_town=  'MANSFIELD' then 'Nottingham-Derby metropolitan area'
            when postcode_town=  'NEWARK' then 'Nottingham-Derby metropolitan area'

            when postcode_town=  'GLASGOW' then 'Glasgow metropolitan area'
            when postcode_town=  'EAST KILBRIDE' then 'Glasgow metropolitan area'
            when postcode_town=  'CUMBERNAULD' then 'Glasgow metropolitan area'
            when postcode_town=  'KILMARNOCK' then 'Glasgow metropolitan area'
            when postcode_town=  'DUMBARTON' then 'Glasgow metropolitan area'

            when postcode_town=  'CARDIFF' then 'Cardiff and South Wales valleys metropolitan area'
            when postcode_town=  'NEWPORT' then 'Cardiff and South Wales valleys metropolitan area'
            when postcode_town=  'MERTHYR TYDFIL' then 'Cardiff and South Wales valleys metropolitan area'
            when postcode_town=  'PONTYPRIDD' then 'Cardiff and South Wales valleys metropolitan area'
            when postcode_town=  'CAERPHILLY' then 'Cardiff and South Wales valleys metropolitan area'
            when postcode_town=  'BRIDGEND' then 'Cardiff and South Wales valleys metropolitan area'
            when postcode_town=  'EBBW VALE' then 'Cardiff and South Wales valleys metropolitan area'

            when postcode_town=  'BRISTOL' then 'Bristol metropolitan area'
            when postcode_town=  'WESTON-SUPER-MARE' then 'Bristol metropolitan area'
            when postcode_town=  'BATH' then 'Bristol metropolitan area'
            when postcode_town=  'CLEVEDON' then 'Bristol metropolitan area'

            when postcode_town=  'BELFAST' then 'Belfast metropolitan area'
            when postcode_town=  'BANGOR' then 'Belfast metropolitan area'

            when postcode_town=  'EDINBURGH' then 'Edinburgh metropolitan area'
            when postcode_town=  'LIVINGSTON' then 'Edinburgh metropolitan area'

            when postcode_town=  'BRIGHTON' then 'Brighton/Worthing/Littlehampton metropolitan area'
            when postcode_town=  'WORTHING' then 'Brighton/Worthing/Littlehampton metropolitan area'
            when postcode_town=  'EASTBOURNE' then 'Brighton/Worthing/Littlehampton metropolitan area'
            when postcode_town=  'LITTLEHAMPTON' then 'Brighton/Worthing/Littlehampton metropolitan area'

            when postcode_town=  'LEICESTER' then 'Leicester metropolitan area'
            when postcode_town=  'LOUGHBOROUGH' then 'Leicester metropolitan area'
            when postcode_town=  'SHEPSHED' then 'Leicester metropolitan area'
            when postcode_town=  'HINCKLEY' then 'Leicester metropolitan area'
            when postcode_town=  'COALVILLE' then 'Leicester metropolitan area'
            when postcode_town=  'MELTON MOWBRAY' then 'Leicester metropolitan area'
            else null
            end) as metropolitan_area
into metro_area
from pc_area_table3;
commit;


select *, (case when metropolitan_area is null
                then barb_desc_itv else metropolitan_area end)
                as metropolitan_area_and_itv_region
into metro_area_n_tv_region
from metro_area;
commit;

drop table metro_area;
commit;

drop table pc_area_table;
commit;

drop table pc_area_table2;
commit;

drop table pc_area_table3;
commit;

CREATE HG INDEX idx15 ON metro_area_n_tv_region(cb_key_household);
commit;




Update AdSmart
set base.metropolitan_area_and_itv_region  = metro.metropolitan_area_and_itv_region
        ,base.pc_area                      = metro.pc_area2
        ,base.postcode_town                = metro.postcode_town
from AdSmart as base inner join metro_area_n_tv_region as metro
on base.cb_key_household = metro.cb_key_household;
commit;




--Make corrections to the data because of CBI requirements

Update AdSmart base
set base.metropolitan_area_and_itv_region = case when base.pc_area in ('JE','GY') then 'Channel Islands'
                                            else base.metropolitan_area_and_itv_region end -- Jersey, Guernsey & Alderney
;
commit;

Update AdSmart base
set base.metropolitan_area_and_itv_region = case when base.metropolitan_area_and_itv_region = 'Meridian (exc. Channel Islands)' then 'Meridian'
                                                 when base.metropolitan_area_and_itv_region = 'North East' then 'North-East'
                                                 when base.metropolitan_area_and_itv_region = 'North West' then 'North-West'
                                                 when base.metropolitan_area_and_itv_region = 'South West' then 'South-West'
                                                 when base.metropolitan_area_and_itv_region = 'East Of England' then 'East-of-England'
                                                 when base.metropolitan_area_and_itv_region = 'North Scotland' then 'Northern Scotland'
                                            else base.metropolitan_area_and_itv_region end
;
commit;

Update AdSmart base
set base.metropolitan_area_and_itv_region = case when (base.metropolitan_area_and_itv_region = 'Border') and
                                                     (base.pc_area in ('AB','DD','DG','EH','FK','G','HS','IV','KA','KY','ML','PA','PH','TD')
                                                     or upper(base.country) = 'SCOTLAND') then 'Border-Scotland'
                                                 when (base.metropolitan_area_and_itv_region = 'Border') and
                                                     (base.pc_area not in ('AB','DD','DG','EH','FK','G','HS','IV','KA','KY','ML','PA','PH','TD')
                                                     or upper(base.country) = 'ENGLAND') then 'Border-England'
                                            else base.metropolitan_area_and_itv_region end

,base.government_region = case when base.government_region = 'Yorkshire and the Humber' then 'Yorkshire and The Humber'
                              else base.government_region end
;
commit;


drop table metro_area_n_tv_region;
commit;



-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################

