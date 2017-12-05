

select count(*) from sk_prodreg.VESPA_KANTAR_SURVEY_DATA ;

exec gen_create_table 'sk_prodreg.VESPA_KANTAR_SURVEY_DATA'


select top 10 *  from sk_prodreg.VESPA_KANTAR_SURVEY_DATA ;
output to 'c:/kantar survey data.xls' format excel;

commit;
