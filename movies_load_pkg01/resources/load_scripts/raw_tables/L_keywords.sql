INSERT INTO [ONLAB].[dbo].[keywords]
SELECT DISTINCT
	  [movieId]
	, [keyword]
FROM [ONLAB].[dbo].[temp_keywords]
;