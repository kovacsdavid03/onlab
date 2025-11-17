INSERT INTO [dbo].[temp_dim_language]
SELECT
	sp.[language]
FROM [dbo].[spoken_languages] sp
LEFT JOIN [dbo].[dim_language] dl ON sp.[language] = dl.[language]
WHERE dl.[language] IS NULL