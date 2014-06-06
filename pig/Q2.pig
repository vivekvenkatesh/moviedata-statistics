/*
 * Load the Ratings File
 */
ratings = LOAD '/Spring2014_HW-3-Pig/ratings_new.dat' USING PigStorage('\u003B') as (uid:int,mid:int,rating:int,timestamp:chararray);

/* 
 * Load the Users Table
 */ 

users = LOAD '/Spring2014_HW-3-Pig/users_new.dat' USING PigStorage('\u003B') as (uid:int,gender:chararray,age:int,occupation:chararray,zipcode:chararray);

/* 
 * Cogroup by User Id
 */ 

cogroup_by_userid = COGROUP users by uid, ratings by uid;
store cogroup_by_userid into '/user/vxg120430/Pig/output2';
