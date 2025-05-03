INSERT INTO [ONLAB].[dbo].[temp_dim_company]
SELECT DISTINCT
	[production_company]
FROM [ONLAB].[dbo].[production_companies]