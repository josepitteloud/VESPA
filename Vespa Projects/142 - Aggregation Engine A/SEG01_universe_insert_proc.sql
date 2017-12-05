



/*******************************************************************************
 **                                                                           **
 **  Manage the universe, assign new uniqid if required.                      **
 **                                                                           **
 **    @params:                                                               **
 **    1. @_table_containing_accounts table name where one of the columns     **
 **       is named account_number and contains account numbers                **
 **    2. @_uniqid is the universe_id assigned to the list of accounts        **
 **                                                                           **
 *******************************************************************************/

CREATE or replace procedure SEG01_universe_insert_proc(
                 in   @_table_containing_accounts   varchar(128),
                out   @_uniqid                      bigint
                ) AS
BEGIN


--check to see if the account_number list already exists as a universe

--how many accounts are we looking for?
declare @num_accounts bigint
execute('select @num_accounts = count(distinct account_number) from '||@_table_containing_accounts)


select uniqid, count(1) sample_count
  into universe_counts_tmp
  from SEG01_universe_tbl
group by uniqid

--do any of the sample_counts match the number of records in the supplied account list
-- if yes, then need to see if they match exactly

execute('select * into universes_of_equal_size_tmp from universe_counts_tmp where sample_count = '||@num_accounts)
commit

--see how many entries there are here
IF (select count(1) from universes_of_equal_size_tmp) > 0 --if there are matching size universes
    BEGIN
        --search through the actual account listings
        --do an outer join between the 2 data sets and see if there are any NULLs

        declare @diff_accounts bigint


        select t.*, a.account_number test_accounts
          into universe_accounts_tmp
          from (select u.*
                  from SEG01_universe_tbl u, universes_of_equal_size_tmp s
                 where u.uniqid = s.uniqid) t,
               table_containing_accounts_tmp a
         where t.account_number =* a.account_number

        --see how many records are diffnt
        select @diff_accounts = count(1)
          from universe_accounts_tmp b
         where b.account_number is null



        --IF NOT THE SAME UNIVERSE
        IF @diff_accounts > 0 --universe does not exist, create it
            BEGIN
                --assign new universe_id
                select @_uniqid = coalesce(max(uniqid),0) +1
                  from SEG01_universe_tbl

                execute(
                    ' INSERT into SEG01_universe_tbl(uniqid, account_number) '||
                    ' select distinct '||@_uniqid||' uniqid, account_number '||
                    '   from '||@_table_containing_accounts||
                    '  where account_number is not null ')
                commit

            END
        ELSE
            BEGIN --universe exists
                select @_uniqid = min(uniqid)
                  from universe_accounts_tmp
            END
    END
ELSE --universe does not exist, create it
    BEGIN
                    --assign new universe_id
                select @_uniqid = coalesce(max(uniqid),0) +1
                  from SEG01_universe_tbl

                execute(
                    ' INSERT into SEG01_universe_tbl(uniqid, account_number) '||
                    ' select distinct '||@_uniqid||' uniqid, account_number '||
                    '   from '||@_table_containing_accounts||
                    '  where account_number is not null ')
                commit
    END

--clean up
IF object_id('universe_counts_tmp') IS NOT NULL
    BEGIN
        DROP TABLE universe_counts_tmp
    END
IF object_id('universe_accounts_tmp') IS NOT NULL
    BEGIN
        DROP TABLE universe_accounts_tmp
    END
IF object_id('universes_of_equal_size_tmp') IS NOT NULL
    BEGIN
        DROP TABLE universes_of_equal_size_tmp
    END
commit
--end cleanup

END;

----- End Proc
------------ ########




----test

create table table_containing_accounts_tmp(
    account_number varchar(24) not null
)

insert into table_containing_accounts_tmp(account_number) values ('621057736251')
insert into table_containing_accounts_tmp(account_number) values ('210128206856')
insert into table_containing_accounts_tmp(account_number) values ('620058025243')
insert into table_containing_accounts_tmp(account_number) values ('620000015573')
insert into table_containing_accounts_tmp(account_number) values ('630033367917')
insert into table_containing_accounts_tmp(account_number) values ('220022800381')
insert into table_containing_accounts_tmp(account_number) values ('210042200829')
insert into table_containing_accounts_tmp(account_number) values ('621010613217')
insert into table_containing_accounts_tmp(account_number) values ('621164999677')
insert into table_containing_accounts_tmp(account_number) values ('620056878221')
commit

---execute proc


declare @uniqid bigint

exec SEG01_universe_insert_proc 'table_containing_accounts_tmp', @uniqid

select @uniqid

--tmp
/*
drop table universe_counts_tmp
drop table universe_accounts_tmp
drop table universes_of_equal_size_tmp
commit
--end tmp drops

--test
select *
from universe_counts_tmp

select *
from universes_of_equal_size_tmp

select *
  from universe_counts_tmp
 where sample_count = 10

*/
--end test
