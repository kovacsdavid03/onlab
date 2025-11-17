INSERT INTO [ONLAB].[dbo].[genres]
SELECT DISTINCT
	[movieId]
	, [genre]
FROM [ONLAB].[dbo].[temp_genres]