//! Pre-defined datasets.

pub static TEST_TABLE: &str = "CREATE TABLE test (id INTEGER PRIMARY KEY, value STRING)";

pub static MOVIES: &str = "
        CREATE TABLE countries (
            id STRING PRIMARY KEY,
            name STRING NOT NULL
        );
        INSERT INTO countries VALUES
            ('fr', 'France'),
            ('ru', 'Russia'),
            ('us', 'United States of America');
        CREATE TABLE genres (
            id INTEGER PRIMARY KEY,
            name STRING NOT NULL
        );
        INSERT INTO genres VALUES
            (1, 'Science Fiction'),
            (2, 'Action'),
            (3, 'Comedy');
        CREATE TABLE studios (
            id INTEGER PRIMARY KEY,
            name STRING NOT NULL,
            country_id STRING REFERENCES countries
        );
        INSERT INTO studios VALUES
            (1, 'Mosfilm', 'ru'),
            (2, 'Lionsgate', 'us'),
            (3, 'StudioCanal', 'fr'),
            (4, 'Warner Bros', 'us');
        CREATE TABLE movies (
            id INTEGER PRIMARY KEY,
            title STRING NOT NULL,
            studio_id INTEGER NOT NULL REFERENCES studios,
            genre_id INTEGER NOT NULL REFERENCES genres,
            released INTEGER NOT NULL,
            rating FLOAT,
            ultrahd BOOLEAN
        );
        INSERT INTO movies VALUES
            (1, 'Stalker', 1, 1, 1979, 8.2, NULL),
            (2, 'Sicario', 2, 2, 2015, 7.6, TRUE),
            (3, 'Primer', 3, 1, 2004, 6.9, NULL),
            (4, 'Heat', 4, 2, 1995, 8.2, TRUE),
            (5, 'The Fountain', 4, 1, 2006, 7.2, FALSE),
            (6, 'Solaris', 1, 1, 1972, 8.1, NULL),
            (7, 'Gravity', 4, 1, 2013, 7.7, TRUE),
            (8, 'Blindspotting', 2, 3, 2018, 7.4, TRUE),
            (9, 'Birdman', 4, 3, 2014, 7.7, TRUE),
            (10, 'Inception', 4, 1, 2010, 8.8, TRUE)";
