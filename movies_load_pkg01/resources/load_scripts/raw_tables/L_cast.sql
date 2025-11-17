INSERT INTO [ONLAB].[dbo].[cast]
SELECT DISTINCT
	  [movieId]
	, [character]
	, [gender]
	, [name]
FROM [ONLAB].[dbo].[temp_cast]