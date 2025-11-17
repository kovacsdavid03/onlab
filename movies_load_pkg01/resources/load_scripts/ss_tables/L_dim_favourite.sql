INSERT INTO [dbo].[dim_favourite]
SELECT 
	tdf.[user_id]
	, fm.[movie_sid]
	, tdf.[created_at]
FROM [dbo].[temp_dim_favourite] tdf
LEFT JOIN [dbo].[fact_movies] fm ON tdf.[movieId] = fm.[movieId]
LEFT JOIN [dbo].[dim_favourite] df ON tdf.[user_id] = df.[user_id] AND fm.[movie_sid] = df.[movie_sid]
WHERE df.[user_id] IS NULL AND df.[movie_sid] IS NULL