INSERT INTO [ONLAB].[dbo].[bridge_language]
SELECT
	[language_sid],
	[movie_sid]
FROM [ONLAB].[dbo].[temp_bridge_language]