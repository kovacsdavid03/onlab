INSERT INTO [ONLAB].[dbo].[temp_bridge_language]
SELECT DISTINCT
	dl.[language_sid],
	fm.[movie_sid]
FROM [ONLAB].[dbo].[spoken_languages] AS sl
JOIN [ONLAB].[dbo].[dim_language] AS dl ON sl.[language] = dl.[language]
JOIN [ONLAB].[dbo].[fact_movies] AS fm ON sl.[movieId] = fm.[movieId]