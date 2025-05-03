INSERT INTO [ONLAB].[dbo].[dim_cast]
SELECT
	[name]
	, [gender]
FROM [ONLAB].[dbo].[temp_dim_cast]