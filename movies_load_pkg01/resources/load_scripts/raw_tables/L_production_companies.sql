INSERT INTO [ONLAB].[dbo].[production_companies]
SELECT DISTINCT
	[movieId]
	, [production_company]
FROM [ONLAB].[dbo].[temp_production_companies]