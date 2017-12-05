

INSERT INTO vespa_analysts.scaling_dialback_intervals
   LOCATION 'DCSLOPSKPRD10_olive_prod.vespa_anaysts' 'SELECT * FROM vespa_analysts.scaling_dialback_intervals';
commit;


INSERT INTO vespa_analysts.scaling_weightings
   LOCATION 'DCSLOPSKPRD10_olive_prod.vespa_anaysts' 'SELECT * FROM vespa_analysts.scaling_weightings';

commit;