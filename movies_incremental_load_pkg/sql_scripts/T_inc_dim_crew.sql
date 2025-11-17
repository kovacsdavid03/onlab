INSERT INTO [dbo].[temp_dim_crew]
SELECT
	c.[name]
	, c.[gender]
FROM [dbo].[crew] c
LEFT JOIN [dbo].[dim_crew] dc ON c.[name] = dc.[name]
WHERE dc.[name] IS NULL;