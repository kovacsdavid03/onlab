INSERT INTO [ONLAB].[dbo].[crew]
SELECT DISTINCT
	[movieId]
	, [department]
	, [gender]
	, [job]
	, [name]
FROM [ONLAB].[dbo].[temp_crew]