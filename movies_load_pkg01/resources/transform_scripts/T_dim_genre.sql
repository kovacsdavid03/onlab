INSERT INTO [ONLAB].[dbo].[temp_dim_genre]
SELECT DISTINCT
	[genre]
FROM [ONLAB].[dbo].[genres]