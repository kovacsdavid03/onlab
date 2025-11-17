INSERT INTO [dbo].[temp_dim_keyword]
SELECT
	k.[keyword]
FROM [dbo].[keywords] k
LEFT JOIN [dbo].[dim_keyword] dk ON k.[keyword] = dk.[keyword]
WHERE dk.[keyword] IS NULL;