INSERT INTO [ONLAB].[dbo].[temp_dim_country]
SELECT DISTINCT
	[production_country]
FROM [ONLAB].[dbo].[production_countries]