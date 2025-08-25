-- Optional: Pre-create tables (the loader also creates them if missing)
CREATE TABLE IF NOT EXISTS genre_stats (
    genre TEXT,
    n_ratings BIGINT,
    avg_rating DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS year_stats (
    year INTEGER,
    n_ratings BIGINT,
    avg_rating DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS top_movies (
    movie_name TEXT,
    n_ratings BIGINT,
    avg_rating DOUBLE PRECISION
);
