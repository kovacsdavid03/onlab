INSERT INTO [ONLAB].[dbo].[bridge_cast]
SELECT
	[cast_sid],
	[movie_sid],
	[character]
FROM [ONLAB].[dbo].[temp_bridge_cast]
