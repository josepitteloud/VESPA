-- Okay, so we got this extra .csv data source which lists the currently active
-- Sky View Panel members or something. This is the second time through, and it
-- looks like we might be managing more of this going forwards (still manually)
-- so we might want to build a more robust path? So this is the loading script:

IF object_id('vespa_analysts.verified_Sky_View_members') IS NOT NULL
    DROP TABLE vespa_analysts.verified_Sky_View_members;
	
create table vespa_analysts.verified_Sky_View_members (
    subscriber_id       	decimal(10,0)   unique
    ,load_date           	date            not null default today()
	,card_subscriber_id 	varchar(12)     unique
	,account_number 		varchar(20)
);


create index for_joining on verified_Sky_View_members (account_number);

grant select on verified_Sky_View_members to vespa_group_low_security, sk_prodreg;

commit;

-- This command only works in Sybase SQL Interactive:
input into vespa_analysts.verified_Sky_View_members (
        subscriber_id
        )
from 'G:\\RTCI\\Sky Projects\\Vespa\\Regulars\\External sources\\Sky View Dashboard\\Kantarpanel_member.csv'
format ascii;

commit;


-- but wait; need to normalise the card_subscriber_id values to length 8, padded by zeros...
update verified_Sky_View_members
set card_subscriber_id = right( replicate('0', 8) || convert(varchar, abs(subscriber_id)), 8);

commit;

-- Filling in account_numbers:

update vespa_analysts.verified_Sky_View_members
set account_number = ccsl.account_number
from vespa_analysts.verified_Sky_View_members as csvm
inner join sk_prod.cust_card_subscriber_link as ccsl
on csvm.card_subscriber_id = ccsl.card_subscriber_id
and today() between effective_from_dt and effective_to_dt;

commit;

/****************** Build QA: *******************/

select count(1) from vespa_analysts.verified_Sky_View_members;
-- 32820
-- On Feb 23rd: 32830

select count(distinct csvm.card_subscriber_id)
from vespa_analysts.verified_Sky_View_members as csvm
inner join sk_prod.cust_card_subscriber_link as ccsl
on csvm.card_subscriber_id = ccsl.card_subscriber_id;
-- 32811 -- missing 9 - close enough.
-- Feb 23rd: 32828 - only missing 2 now

select count(distinct csvm.card_subscriber_id)
from vespa_analysts.verified_Sky_View_members as csvm
inner join sk_prod.CUST_SERVICE_INSTANCE as ccsl
on csvm.card_subscriber_id = ccsl.si_external_identifier;
-- 32808 -- missing 12 - again, I don't care.
-- Feb 23: 32825 - mising 5, close enough

-- So, that's close enough. We'll go with it.

select count(distinct csvm.card_subscriber_id)
from vespa_analysts.verified_Sky_View_members as csvm
inner join sk_prod.cust_card_subscriber_link as ccsl
on csvm.card_subscriber_id = ccsl.card_subscriber_id
and '2012-02-02' between effective_from_dt and effective_to_dt
-- Feb 23rd: 32809 - missing like 20, w/e

