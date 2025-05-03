INSERT INTO [ONLAB].[dbo].[temp_dim_crew]
SELECT DISTINCT
	[name]
	, [gender]
FROM [ONLAB].[dbo].[crew]