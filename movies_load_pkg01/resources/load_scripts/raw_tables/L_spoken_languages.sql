INSERT INTO [ONLAB].[dbo].[spoken_languages]
SELECT DISTINCT
	[movieId]
	, [language]
FROM [ONLAB].[dbo].[temp_spoken_languages]