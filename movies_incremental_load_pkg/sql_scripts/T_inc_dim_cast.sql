INSERT INTO [dbo].[temp_dim_cast]
SELECT
	c.[name]
	, c.[gender]
FROM [dbo].[cast] c
LEFT JOIN [dbo].[dim_cast] dc ON c.[name] = dc.[name]
WHERE dc.[name] IS NULL;