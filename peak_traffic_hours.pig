-- TASK 2: IDENTIFY PEAK TRAFFIC HOURS


-- Load the CSV data with proper schema
A = LOAD '/user/hadoop/log_analysis/input/extended_server_logs.csv' 
    AS (ip:chararray, user:chararray, timestamp:chararray, 
        method:chararray, resource:chararray, protocol:chararray, 
        status:int, size:int);

B = FOREACH A GENERATE 
    SUBSTRING(timestamp, 12, 2) as hour, 
    resource;

-- Group by hour
C = GROUP B BY hour;

-- Count requests per hour
D = FOREACH C GENERATE group as traffic_hour, COUNT(B) as requests;

-- Sort by request count in descending order (peak hours first)
E = ORDER D BY requests DESC;

-- Display results
DUMP E;

-- Store results to HDFS
STORE E INTO '/user/hadoop/log_analysis/pig_output/peak_traffic_hours';
