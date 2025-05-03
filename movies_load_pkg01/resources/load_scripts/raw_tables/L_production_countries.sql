INSERT INTO [ONLAB].[dbo].[production_countries]
SELECT
	[movieId]
	, [production_country]
FROM [ONLAB].[dbo].[temp_production_countries]