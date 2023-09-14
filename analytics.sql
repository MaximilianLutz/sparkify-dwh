-- Dataset  overview:
SELECT count(*) FROM "dev"."public"."songplays";
SELECT * FROM "dev"."public"."songplays" ORDER BY start_time desc;

-- Number of daily users per day  
Select date_trunc('day', start_time), count(distinct user_id) FROM songplays
GROUP BY 1  
ORDER BY 1

-- Distribution of subscription levels  
Select level, count(distinct user_id) FROM songplays
GROUP BY 1  
ORDER BY 1

-- Most popular artists  
Select a.name, count(songplay_id) FROM songplays as sp
LEFT JOIN artists as a on sp.artist_id = a.artist_id
GROUP BY 1 
ORDER BY 2 desc 

-- Most popular songs  
Select s.title, count(songplay_id) FROM songplays as sp
LEFT JOIN songs as s on sp.song_id = s.song_id
GROUP BY 1 
ORDER BY 2 desc 