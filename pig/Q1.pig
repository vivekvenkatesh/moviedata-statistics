/*
 * Load Users Input File
 * Get the female users whose age between 20 and 30
 *
 */ 

users = LOAD '/Spring2014_HW-3-Pig/users_new.dat' USING PigStorage('\u003B') as (uid:int,gender:chararray,age:int,occupation:chararray,zipcode:chararray); 
users_female = FILTER users by (gender matches 'F') AND (age >= 20 AND age <=30);

/* 
 * Load Movies Input File
 * Get the Action and War Movies
 *
 */

movies = LOAD '/Spring2014_HW-3-Pig/movies_new.dat' USING PigStorage('\u003B') as (mid:int,title:chararray,genre:chararray);
movies_action_war = FILTER movies by ((genre matches '.*Action.*') AND (genre matches '.*War.*'));

/* 
 * Load the Ratings Input File
 * Join the Ratings and Filtered Movies input file based on MID
 */ 

ratings = LOAD '/Spring2014_HW-3-Pig/ratings_new.dat' USING PigStorage('\u003B') as (uid:int,mid:int,rating:int,timestamp:chararray);

filter_ratings_movie = COGROUP movies_action_war by mid, ratings by mid; 
filter_ratings_movie_remove_null = FILTER filter_ratings_movie by ((not IsEmpty(movies_action_war)) AND (not IsEmpty(ratings)));

average_rating = FOREACH filter_ratings_movie_remove_null GENERATE group as mid, AVG(ratings.rating) as AverageRating, ratings.uid as RatedUsers;
group_average_rating = GROUP average_rating ALL; 
max_average_rating = FOREACH group_average_rating GENERATE MAX(average_rating.AverageRating) as max; 
get_movie_with_max_average_rating = FILTER average_rating BY AverageRating == (double)max_average_rating.max;  
get_movie_with_max_average_rating_flattened = FOREACH get_movie_with_max_average_rating GENERATE mid, AverageRating, flatten(RatedUsers);  
female_users_who_rated_movie_with_max_rating = JOIN get_movie_with_max_average_rating_flattened by RatedUsers::uid, users_female by uid; 
final = FOREACH female_users_who_rated_movie_with_max_rating GENERATE users_female::uid; 
store final into '/user/vxg120430/Pig/output1';
