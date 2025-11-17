INSERT INTO [dbo].[temp_fact_movies]
SELECT 
    m.[movieId]
   , m.[imdbId]
   , m.[adult]
   , m.[budget]
   , m.[original_title]
   , m.[popularity]
   , m.[release_date]
   , m.[revenue]
   , m.[runtime]
   , m.[tagline]
   , m.[vote_average]
   , m.[vote_count]
FROM movies m
LEFT JOIN fact_movies fm ON m.movieId = fm.movieId
WHERE fm.movieId IS NULL;