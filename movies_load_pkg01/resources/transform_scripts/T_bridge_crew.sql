INSERT INTO [ONLAB].[dbo].[temp_bridge_crew]
SELECT DISTINCT
	dc.[crew_sid],
	fm.[movie_sid],
	c.[department],
	c.[job]
FROM [ONLAB].[dbo].[crew] AS c
JOIN [ONLAB].[dbo].[dim_crew] AS dc ON c.[name] = dc.[name] AND c.[gender] = dc.[gender]
JOIN [ONLAB].[dbo].[fact_movies] AS fm ON c.[movieId] = fm.[movieId]