INSERT INTO [ONLAB].[dbo].[production_countries]
SELECT DISTINCT
	[movieId]
	, [production_country]
FROM [ONLAB].[dbo].[temp_production_countries]