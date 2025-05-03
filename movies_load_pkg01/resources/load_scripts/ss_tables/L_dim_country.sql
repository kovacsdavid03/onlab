INSERT INTO [ONLAB].[dbo].[dim_country]
SELECT DISTINCT
	[country]
FROM [ONLAB].[dbo].[temp_dim_country]