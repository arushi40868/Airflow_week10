CREATE SCHEMA IF NOT EXISTS books;

-- Raw-like staging tables (loaded via COPY)
CREATE TABLE IF NOT EXISTS books.metadata_raw (
asin TEXT PRIMARY KEY,
title TEXT,
authors TEXT,
categories TEXT,
price NUMERIC,
rating NUMERIC,
total_reviews INT,
link TEXT,
image TEXT
);

CREATE TABLE IF NOT EXISTS books.reviews_raw (
review_id TEXT PRIMARY KEY,
asin TEXT,
rating NUMERIC,
title TEXT,
body TEXT,
helpful_votes INT,
date DATE,
reviewer_id TEXT,
reviewer_name TEXT
);

-- Cleaned/curated tables
CREATE TABLE IF NOT EXISTS books.metadata_clean AS SELECT * FROM books.metadata_raw WHERE false;
CREATE TABLE IF NOT EXISTS books.reviews_clean AS SELECT * FROM books.reviews_raw WHERE false;

-- Final merged table (1 row per review joined with book attrs)
CREATE TABLE IF NOT EXISTS books.book_reviews AS
SELECT * FROM (SELECT 1) t WHERE false;

-- Analysis table
CREATE TABLE IF NOT EXISTS books.book_stats (
asin TEXT,
title TEXT,
author TEXT,
reviews INT,
avg_rating NUMERIC,
pct_5star NUMERIC,
PRIMARY KEY (asin)
);