INSERT INTO [ONLAB].[dbo].[temp_bridge_genre]
SELECT DISTINCT
	dg.[genre_sid],
	fm.[movie_sid]
FROM [ONLAB].[dbo].[genres] AS g
JOIN [ONLAB].[dbo].[dim_genre] AS dg ON g.[genre] = dg.[genre]
JOIN [ONLAB].[dbo].[fact_movies] AS fm ON g.[movieId] = fm.[movieId]