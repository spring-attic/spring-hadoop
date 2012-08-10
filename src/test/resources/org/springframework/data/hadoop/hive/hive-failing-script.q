DROP TABLE u_data;
CREATE EXTERNAL TABLE u_data(userid INT,movieid INT,rating INT,unixtime STRING)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
LOAD DATA INPATH '/tmp/u.data' OVERWRITE INTO TABLE u_data;
select movieid, avg(rating) as avg_r from u_data group by movieid order by avg_r;


