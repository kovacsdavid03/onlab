INSERT INTO [dbo].[temp_dim_country]
SELECT DISTINCT 
	pc.[production_country] AS [country]
FROM production_countries pc
LEFT JOIN [dbo].[dim_country] dc ON pc.[production_country] = dc.[country]
WHERE dc.[country] IS NULL;