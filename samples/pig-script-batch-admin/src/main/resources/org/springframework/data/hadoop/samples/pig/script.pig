a = load 'u.data' as (userid:int, movieid:int, rating:int, unixtime:chararray);
good_rating = filter a by rating > 3;
limited_ten = LIMIT good_rating 10;
dump limited_ten;
