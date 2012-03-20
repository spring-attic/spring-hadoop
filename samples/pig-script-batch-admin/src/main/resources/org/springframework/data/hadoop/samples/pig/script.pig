a = load 'u.data' as (userid:int, movieid:int, rating:int, unixtime:chararray);
good_rating = filter a by rating > 3;
user_rating = group good_rating by movieid;
dump user_rating;