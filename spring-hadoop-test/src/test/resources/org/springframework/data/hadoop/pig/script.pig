A = LOAD 'src/test/resources/logs/apache_access.log' USING PigStorage() AS (name:chararray, age:int);
B = FOREACH A GENERATE name;
DUMP B;
