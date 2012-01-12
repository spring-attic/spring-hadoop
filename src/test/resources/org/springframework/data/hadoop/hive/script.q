drop table if exists testHiveBatchTable; 
create table testHiveBatchTable (key int, value string);
show tables;
select count(1) from testHiveBatchTable;