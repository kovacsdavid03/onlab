INSERT INTO [dbo].[temp_dim_company]
SELECT DISTINCT 
	pc.[production_company] AS [company]
FROM production_companies pc
LEFT JOIN [dbo].[dim_company] dc ON pc.[production_company] = dc.[company]
WHERE dc.[company] IS NULL;