INSERT INTO [ONLAB].[dbo].[bridge_country]
SELECT
	   [country_sid],
       [movie_sid]
FROM [ONLAB].[dbo].[temp_bridge_country]