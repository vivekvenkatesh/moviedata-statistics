REGISTER /home/004/v/vx/vxg120430/FormatGenre.jar; 

movies = LOAD '/Spring2014_HW-3-Pig/movies_new.dat' USING PigStorage('\u003B') as (mid:int,title:chararray,genre:chararray);

movies_format_genre = FOREACH movies GENERATE mid, title, FormatGenre(genre); 
store movies_format_genre INTO '/user/vxg120430/Pig/output4';
