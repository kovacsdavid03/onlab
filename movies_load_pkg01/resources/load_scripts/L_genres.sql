INSERT INTO [ONLAB].[dbo].[genres]
SELECT
	[movieId]
	, [genre]
FROM [ONLAB].[dbo].[temp_genres]