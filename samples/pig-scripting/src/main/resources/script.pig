a = LOAD 'u.data' as (userid:int, movieid:int, rating:int, unixtime:chararray);
top_rating = FILTER a by rating > 4;
user_rating = GROUP top_rating BY movieid;
top_three = LIMIT user_rating 3;
DUMP top_three;

