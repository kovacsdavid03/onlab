INSERT INTO [ONLAB].[dbo].[temp_dim_language]
SELECT DISTINCT
	[language]
FROM [ONLAB].[dbo].[spoken_languages]