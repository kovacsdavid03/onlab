INSERT INTO [ONLAB].[dbo].[bridge_crew]
SELECT
	[crew_sid],
	[movie_sid],
	[department],
	[job]
FROM [ONLAB].[dbo].[temp_bridge_crew]