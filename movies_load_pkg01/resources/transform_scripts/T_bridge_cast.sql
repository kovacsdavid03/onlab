INSERT INTO [ONLAB].[dbo].[temp_bridge_cast]
SELECT DISTINCT
	dc.[cast_sid],
	fm.[movie_sid],
	c.[character]
FROM [ONLAB].[dbo].[dim_cast] as dc
INNER JOIN [ONLAB].[dbo].[cast] as c ON dc.[name] = c.[name] and dc.[gender] = c.[gender]
INNER JOIN [ONLAB].[dbo].[fact_movies] as fm ON c.[movieId] = fm.[movieId]
WHERE dc.[cast_sid] IS NOT NULL
