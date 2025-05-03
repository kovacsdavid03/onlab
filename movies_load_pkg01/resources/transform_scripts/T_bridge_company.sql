INSERT INTO [ONLAB].[dbo].[temp_bridge_company]
SELECT DISTINCT
	dc.[company_sid],
	fm.[movie_sid]
FROM [ONLAB].[dbo].[production_companies] AS pc
JOIN [ONLAB].[dbo].[dim_company] AS dc ON pc.[production_company] = dc.[company]
JOIN [ONLAB].[dbo].[fact_movies] as fm ON pc.[movieId]=fm.[movieId]
