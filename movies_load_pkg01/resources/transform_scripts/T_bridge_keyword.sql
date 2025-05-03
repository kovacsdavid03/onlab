INSERT INTO [ONLAB].[dbo].[temp_bridge_keyword]
SELECT DISTINCT
	fm.[movie_sid],
	dk.[keyword_sid]
FROM [ONLAB].[dbo].[keywords] AS k
JOIN [ONLAB].[dbo].[dim_keyword] AS dk ON k.[keyword] = dk.[keyword]
JOIN [ONLAB].[dbo].[fact_movies] AS fm ON k.[movieId] = fm.[movieId]