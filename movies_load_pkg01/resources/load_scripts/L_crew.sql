INSERT INTO [ONLAB].[dbo].[crew]
SELECT
	[movieId]
	, [department]
	, [gender]
	, [job]
	, [name]
FROM [ONLAB].[dbo].[temp_crew]