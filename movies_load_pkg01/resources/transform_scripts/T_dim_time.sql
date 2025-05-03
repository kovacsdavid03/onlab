INSERT INTO [ONLAB].[dbo].[temp_dim_time]
SELECT DISTINCT
  [release_date] AS [date],
  YEAR([release_date]) AS [year],
  MONTH([release_date]) AS [month],
  DAY([release_date]) AS [day],
  DATENAME(WEEKDAY, [release_date]) AS [day_of_week],
  DATEPART(WEEK, [release_date]) AS [week_of_year]
FROM [ONLAB].[dbo].movies
WHERE [release_date] IS NOT NULL