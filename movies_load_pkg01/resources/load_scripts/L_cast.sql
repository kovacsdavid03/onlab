INSERT INTO [ONLAB].[dbo].[cast]
SELECT
	  [movieId]
	, [character]
	, [gender]
	, [name]
FROM [ONLAB].[dbo].[temp_cast]