INSERT INTO [dbo].[temp_dim_genre]
SELECT
	g.[genre]
FROM [dbo].[genres] g
LEFT JOIN [dbo].[dim_genre] dg ON g.[genre] = dg.[genre]
WHERE dg.[genre] IS NULL;