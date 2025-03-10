INSERT INTO [ONLAB].[dbo].[keywords]
SELECT
	  [movieId]
	, [keyword]
FROM [ONLAB].[dbo].[temp_keywords]
;