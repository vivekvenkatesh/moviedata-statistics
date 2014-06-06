/*
 * Load users_new file
 */
users = LOAD '/Spring2014_HW-3-Pig/users_new.dat' USING PigStorage('\u003B') as (uid,gender,age,occupation,zipcode);

/*
 * Load ratings file
 */
ratings = LOAD '/Spring2014_HW-3-Pig/ratings_new.dat' USING PigStorage('\u003B') as (uid,mid,rating,timestamp);

/*
 * Implementing Join of both the above files based in UserId by using COGROUP command
 */

cogroup_users_ratings = COGROUP users by uid, ratings by uid; 

/*
 * cogroup plus foreach, where each bag is flattened, is equivalent to a join—as long as there are no null values in the keys
 */ 
final = foreach cogroup_users_ratings generate flatten(users), flatten(ratings); 
store final into '/user/vxg120430/Pig/output3';
