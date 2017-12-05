

-- for D12 accounts

select account_number, card_id
into #D12_tmp_accounts
from
CUST_CARD_ISSUE_DIM
where
card_id in
(
'701399222'
,'701421612'
,'701458663'
,'701399248'
,'701399198'
,'701420762'
,'701399271'
,'701399305'
,'701398919'
,'701399297'
,'701399065'
,'701399099'
,'701420986'
,'701421547'
,'701420770'
,'701399206'
,'701399289'
,'701399313'
,'701399255'
,'701399263'
,'701420747'
,'701420754'
,'701458515'
,'701414203'
,'701458713'
,'701458465'
,'701459430'
,'701458671'
,'701458473'
,'701458481'
,'701458499'
,'701458507'
)
order by card_id

select d12.account_number
,d12.card_id
,cst.decoder_nds_number
,box_installed_dt
,row_number() over (partition by d12.account_number,d12.card_id order by box_installed_dt desc) as row_nr
into #D12_tmp_accounts2
from
#D12_tmp_accounts d12
inner join  cust_set_top_box            as  cst     on  d12.account_number     =   cst.account_number
                                                        and cst.x_active_box_flag_new   =   'Y'
inner join  cust_single_account_view    as  sav     on  cst.account_number  =   sav.account_number
                                                        and sav.cust_active_dtv =   1
                                                        and sav.prod_latest_dtv_status_code in ('AB','AC','PC')


select * from #D12_tmp_accounts2
--drop table D12_accounts
/*
only take last record
*/
select account_number
,card_id
,decoder_nds_number
,box_installed_dt
,cast(NULL as varchar(50) as Name
,cast(NULL as varchar(50) as Surname
into D12_accounts
from #D12_tmp_accounts2
where row_nr=1
order by card_id

/* correct manually the entries that we can (info from Excel sheet) */

update D12_accounts
set decoder_nds_number='32B0550480007809'
,mismatch_corrected=1
where card_id='701420747'
;

update D12_accounts
set decoder_nds_number='32B0550480007733'
,mismatch_corrected=1
where card_id='701420770'
;

update D12_accounts
set decoder_nds_number='32B0550480007720'
,mismatch_corrected=1
where card_id='701458515'
;
--select * from D12_accounts

/*
add name/surname info
*/
update D12_accounts
set Name = case card_id
         when '701398919' then  'Steve'
               when '701399065' then  'Stuart'
               when '701399099' then  'Stephen'
               when '701399198' then  'Mark'
               when '701399206' then  'Steve'
               when '701399222' then  'Caroline'
               when '701399248' then  'Jonathan'
               when '701399255' then  'Chris'
               when '701399263' then  'Stephen'
               when '701399271' then  'Richard'
               when '701399289' then  'Finn'
               when '701399297' then  'Brad'
               when '701399305' then  'Steve'
               when '701399313' then  'David'
               when '701414203' then  'Nima'
               when '701420747' then  'Simon'
               when '701420754' then  'Jon'
               when '701420762' then  'Neil'
               when '701420770' then  'Alasdair'
               when '701420986' then  'Jon'
               when '701421547' then  'Joe'
               when '701421612' then  'Steve'
               when '701458465' then  'Chris'
               when '701458473' then  'Andrew'
               when '701458481' then  'Alex'
               when '701458499' then  'Dave'
               when '701458507' then  'Roger'
               when '701458515' then  'Stephen'
               when '701458663' then  'Santosh'
               when '701458671' then  'Rob'
               when '701458713' then  'Stuart'
               when '701459430' then  'Neil'
               else NULL
                   end
,Surname = case card_id
         when '701398919' then  'Pope'
               when '701399065' then  'Keeley'
               when '701399099' then  'McDonald'
               when '701399198' then  'Smith'
               when '701399206' then  'Fifield'
               when '701399222' then  'Cardozo'
               when '701399248' then  'Ellisdon'
               when '701399255' then  'Garrett'
               when '701399263' then  'Beattie'
               when '701399271' then  'Parsons'
               when '701399289' then  'Ryan '
               when '701399297' then  'Chodos - Irvine'
               when '701399305' then  'Craig'
               when '701399313' then  'Procter'
               when '701414203' then  'Patel'
               when '701420747' then  'Hatcher'
               when '701420754' then  'Fernyhough'
               when '701420762' then  'Preston'
               when '701420770' then  'Arthur'
               when '701420986' then  'Mitchell'
               when '701421547' then  'Springer'
               when '701421612' then  'Griffith'
               when '701458465' then  'Moore'
               when '701458473' then  'Olson'
               when '701458481' then  'Glass'
               when '701458499' then  'Cameron'
               when '701458507' then  'Lambert'
               when '701458515' then  'O''Boyle'
               when '701458663' then  'Barot'
               when '701458671' then  'Winterschladen'
               when '701458713' then  'McGeechan'
               when '701459430' then  'Garrett'
               else NULL
                        end


/* add info for the D12 into et_technical, we build a new table just in case */
select *
, cast(NULL as bit) as d12
into et_technical_d12
from et_technical
;

update et_technical_d12
set d12=cast(1 as bit)
from
et_technical_d12 et
inner join
D12_accounts acc
on
et.id=acc.decoder_nds_number
;

/*
select reference_date
,count(distinct id) as nrOfD12Devices
,count() as recordsFromD12
from
et_technical_d12
where d12=1
group by reference_date
order by reference_date
;
*/

grant select on D12_accounts to tanghoi, mho08;
grant select on et_technical_d12 to tanghoi, mho08;



