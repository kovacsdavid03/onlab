INSERT INTO [ONLAB].[dbo].[temp_bridge_country]
SELECT DISTINCT
	dc.[country_sid],
	fm.[movie_sid]
FROM [ONLAB].[dbo].[production_countries] AS pc
JOIN [ONLAB].[dbo].[dim_country] AS dc ON pc.[production_country] = dc.[country]
JOIN [ONLAB].[dbo].[fact_movies] AS fm ON pc.[movieId] = fm.[movieId]