INSERT INTO [ONLAB].[dbo].[temp_dim_cast]
SELECT DISTINCT
	[name]
	, [gender]
FROM [ONLAB].[dbo].[cast]