INSERT INTO [ONLAB].[dbo].[dim_crew]
SELECT
	[name]
	, [gender]
FROM [ONLAB].[dbo].[temp_dim_crew]
