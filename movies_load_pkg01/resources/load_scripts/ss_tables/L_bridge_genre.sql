INSERT INTO [ONLAB].[dbo].[bridge_genre]
SELECT
	[genre_sid],
	[movie_sid]
FROM [ONLAB].[dbo].[temp_bridge_genre]