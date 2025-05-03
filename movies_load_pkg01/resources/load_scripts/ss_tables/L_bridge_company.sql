INSERT INTO [ONLAB].[dbo].[bridge_company]
SELECT
	 [company_sid],
     [movie_sid]
 FROM [ONLAB].[dbo].[temp_bridge_company]
