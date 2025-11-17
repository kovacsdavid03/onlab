INSERT INTO [dbo].temp_dim_favourite (user_id, movieId, created_at)
SELECT
	f.[user_id]
	, fm.movieId
	, f.[created_at]
FROM [dbo].Favorites AS f
LEFT JOIN [dbo].[fact_movies] fm ON f.movie_id = fm.movieId