INSERT INTO [ONLAB].[dbo].[dim_time]
SELECT
	  [date]
      ,[year]
      ,[month]
      ,[day]
      ,[day_of_week]
      ,[week_of_year]
FROM [ONLAB].[dbo].[temp_dim_time]
