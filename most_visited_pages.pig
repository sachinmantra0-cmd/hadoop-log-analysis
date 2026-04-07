
-- TASK 1: FIND MOST VISITED PAGES


-- Load the CSV data with proper schema
A = LOAD '/user/hadoop/log_analysis/input/extended_server_logs.csv' 
    AS (ip:chararray, user:chararray, timestamp:chararray, 
        method:chararray, resource:chararray, protocol:chararray, 
        status:int, size:int);

-- Filter for successful requests only (HTTP 200)
B = FILTER A BY status == 200;

-- Group by resource/page
C = GROUP B BY resource;

-- Count visits per page
D = FOREACH C GENERATE group as page, COUNT(B) as visits;

-- Sort by visit count in descending order
E = ORDER D BY visits DESC;

-- Display results
DUMP E;

-- Store results to HDFS
STORE E INTO '/user/hadoop/log_analysis/pig_output/most_visited_pages';
