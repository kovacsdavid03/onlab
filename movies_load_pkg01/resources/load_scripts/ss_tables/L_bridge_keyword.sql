INSERT INTO [ONLAB].[dbo].[bridge_keyword]
SELECT
	[keyword_sid],
	[movie_sid]
FROM [ONLAB].[dbo].[temp_bridge_keyword]