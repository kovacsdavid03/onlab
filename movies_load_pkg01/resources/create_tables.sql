USE ONLAB
GO

CREATE TABLE [temp_cast](
	[movieId] INT,
	[character] NVARCHAR(255),
	[gender] INT,
	[name] NVARCHAR(255)
)

CREATE TABLE [cast](
	[movieId] INT,
	[character] NVARCHAR(255),
	[gender] INT,
	[name] NVARCHAR(255)
)

CREATE TABLE [temp_crew](
	[movieId] INT,
	[department] NVARCHAR(255),
	[gender] INT,
	[job] NVARCHAR(255),
	[name] NVARCHAR(255)
)

CREATE TABLE [crew](
	[movieId] INT,
	[department] NVARCHAR(255),
	[gender] INT,
	[job] NVARCHAR(255),
	[name] NVARCHAR(255)
)

CREATE TABLE [temp_genres](
	[movieId] INT,
	[genre] NVARCHAR(50)
)

CREATE TABLE [genres](
	[movieId] INT,
	[genre] NVARCHAR(50)
)

CREATE TABLE [temp_keywords](
	[movieId] INT,
	[keyword] NVARCHAR(100)
)

CREATE TABLE [keywords](
	[movieId] INT,
	[keyword] NVARCHAR(100)
)

CREATE TABLE [temp_movies](
	[movieId] INT,
	[imdbId] NVARCHAR(10),
	[adult] NVARCHAR(20),
	[budget] INT,
	[original_title] NVARCHAR(255),
	[popularity] DECIMAL(18,6),
	[release_date] DATE,
	[revenue] DECIMAL(18,6),
	[runtime] INT,
	[tagline] NVARCHAR(500),
	[vote_average] DECIMAL(18,6),
	[vote_count] INT
)

CREATE TABLE [movies](
	[movieId] INT,
	[imdbId] NVARCHAR(10),
	[adult] NVARCHAR(20),
	[budget] INT,
	[original_title] NVARCHAR(255),
	[popularity] DECIMAL(18,6),
	[release_date] DATE,
	[revenue] DECIMAL(18,6),
	[runtime] INT,
	[tagline] NVARCHAR(500),
	[vote_average] DECIMAL(18,6),
	[vote_count] INT
)

CREATE TABLE [temp_production_companies](
	[movieId] INT,
	[production_company] NVARCHAR(255)
)

CREATE TABLE [production_companies](
	[movieId] INT,
	[production_company] NVARCHAR(255)
)

CREATE TABLE [temp_production_countries](
	[movieId] INT,
	[production_country] NVARCHAR(255)
)

CREATE TABLE [production_countries](
	[movieId] INT,
	[production_country] NVARCHAR(255)
)

CREATE TABLE [temp_spoken_languages](
	[movieId] INT,
	[language] NVARCHAR(255)
)

CREATE TABLE [spoken_languages](
	[movieId] INT,
	[language] NVARCHAR(255)
)

CREATE TABLE [temp_dim_cast](
	[name] NVARCHAR(255),
	[gender] INT
)

CREATE TABLE [dim_cast](
	[cast_sid] INT IDENTITY(1,1) PRIMARY KEY,
	[name] NVARCHAR(255),
	[gender] INT
)

CREATE TABLE [temp_dim_company](
	[company] NVARCHAR(255)
)

CREATE TABLE [dim_company](
	[company_sid] INT IDENTITY(1,1) PRIMARY KEY,
	[company] NVARCHAR(255)
)

CREATE TABLE [temp_dim_country](
	[country] NVARCHAR(255)
)

CREATE TABLE [dim_country](
	[country_sid] INT IDENTITY(1,1) PRIMARY KEY,
	[country] NVARCHAR(255)
)

CREATE TABLE [temp_dim_crew](
	[name] NVARCHAR(255),
	[gender] INT
)

CREATE TABLE [dim_crew](
	[crew_sid] INT IDENTITY(1,1) PRIMARY KEY,
	[name] NVARCHAR(255),
	[gender] INT
)

CREATE TABLE [temp_dim_genre](
	[genre] NVARCHAR(255)
)

CREATE TABLE [dim_genre](
	[genre_sid] INT IDENTITY(1,1) PRIMARY KEY,
	[genre] NVARCHAR(255)
)

CREATE TABLE [temp_dim_keyword](
	[keyword] NVARCHAR(100)
)

CREATE TABLE [dim_keyword](
	[keyword_sid] INT IDENTITY(1,1) PRIMARY KEY,
	[keyword] NVARCHAR(100)
)

CREATE TABLE [temp_dim_language](
	[language] NVARCHAR(255)
)

CREATE TABLE [dim_language](
	[language_sid] INT IDENTITY(1,1) PRIMARY KEY,
	[language] NVARCHAR(255)
)

CREATE TABLE [temp_dim_time](
	[date] DATE,
	[year] INT,
	[month] INT,
	[day] INT,
	[day_of_week] NVARCHAR(20),
	[week_of_year] INT
)

CREATE TABLE [dim_time](
	[date] DATE,
	[year] INT,
	[month] INT,
	[day] INT,
	[day_of_week] NVARCHAR(20),
	[week_of_year] INT
)

CREATE TABLE [temp_fact_movies](
	[movieId] INT,
	[imdbId] NVARCHAR(10),
	[adult] NVARCHAR(20),
	[budget] INT,
	[original_title] NVARCHAR(255),
	[popularity] DECIMAL(18,6),
	[release_date] DATE,
	[revenue] DECIMAL(18,6),
	[runtime] INT,
	[tagline] NVARCHAR(500),
	[vote_average] DECIMAL(18,6),
	[vote_count] INT
)

CREATE TABLE [fact_movies](
	[movie_sid] INT IDENTITY(1,1) PRIMARY KEY,
	[movieId] INT,
	[imdbId] NVARCHAR(10),
	[adult] NVARCHAR(20),
	[budget] INT,
	[original_title] NVARCHAR(255),
	[popularity] DECIMAL(18,6),
	[release_date] DATE,
	[revenue] DECIMAL(18,6),
	[runtime] INT,
	[tagline] NVARCHAR(500),
	[vote_average] DECIMAL(18,6),
	[vote_count] INT
)

CREATE TABLE [temp_bridge_cast](
	[cast_sid] INT,
	[movie_sid] INT,
	[character] NVARCHAR(255)
)

CREATE TABLE [bridge_cast](
	[cast_sid] INT FOREIGN KEY REFERENCES dim_cast([cast_sid]),
	[movie_sid] INT FOREIGN KEY REFERENCES fact_movies([movie_sid]),
	[character] NVARCHAR(255)
)

CREATE TABLE [temp_bridge_company](
	[company_sid] INT,
	[movie_sid] INT
)

CREATE TABLE [bridge_company](
	[company_sid] INT FOREIGN KEY REFERENCES dim_company([company_sid]),
	[movie_sid] INT FOREIGN KEY REFERENCES fact_movies([movie_sid])
)

CREATE TABLE [temp_bridge_country](
	[country_sid] INT,
	[movie_sid] INT
)

CREATE TABLE [bridge_country](
	[country_sid] INT FOREIGN KEY REFERENCES dim_country([country_sid]),
	[movie_sid] INT FOREIGN KEY REFERENCES fact_movies([movie_sid])
)

CREATE TABLE [temp_bridge_crew](
	[crew_sid] INT,
	[movie_sid] INT,
	[department] NVARCHAR(255),
	[job] NVARCHAR(255)
)

CREATE TABLE [bridge_crew](
	[crew_sid] INT FOREIGN KEY REFERENCES dim_crew([crew_sid]),
	[movie_sid] INT FOREIGN KEY REFERENCES fact_movies([movie_sid]),
	[department] NVARCHAR(255),
	[job] NVARCHAR(255)
)

CREATE TABLE [temp_bridge_genre](
	[genre_sid] INT,
	[movie_sid] INT
)

CREATE TABLE [bridge_genre](
	[genre_sid] INT FOREIGN KEY REFERENCES dim_genre([genre_sid]),
	[movie_sid] INT FOREIGN KEY REFERENCES fact_movies([movie_sid])
)

CREATE TABLE [temp_bridge_keyword](
	[keyword_sid] INT,
	[movie_sid] INT
)

CREATE TABLE [bridge_keyword](
	[keyword_sid] INT FOREIGN KEY REFERENCES dim_keyword([keyword_sid]),
	[movie_sid] INT FOREIGN KEY REFERENCES fact_movies([movie_sid])
)

CREATE TABLE [temp_bridge_language](
	[language_sid] INT,
	[movie_sid] INT
)

CREATE TABLE [bridge_language](
	[language_sid] INT FOREIGN KEY REFERENCES dim_language([language_sid]),
	[movie_sid] INT FOREIGN KEY REFERENCES fact_movies([movie_sid])
)
