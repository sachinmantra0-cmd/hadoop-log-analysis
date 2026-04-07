
-- TASK 3: DETECT SUSPICIOUS IP ACTIVITY


-- Load the CSV data 
A = LOAD '/user/hadoop/log_analysis/input/extended_server_logs.csv' 
    AS (ip:chararray, user:chararray, timestamp:chararray, 
        method:chararray, resource:chararray, protocol:chararray, 
        status:int, size:int);

-- Filter for suspicious activity:
B = FILTER A BY (status == 403) OR 
                (LOWER(resource) LIKE '%admin%') OR
                (LOWER(resource) LIKE '%config%') OR
                (LOWER(resource) LIKE '%password%') OR
                (LOWER(resource) LIKE '%passwd%') OR
                (LOWER(resource) LIKE '%shadow%') OR
                (LOWER(resource) LIKE '%phpmyadmin%') OR
                (LOWER(resource) LIKE '%cpanel%') OR
                (LOWER(resource) LIKE '%sql%') OR
                (LOWER(resource) LIKE '%inject%') OR
                (LOWER(resource) LIKE '%xmlrpc%') OR
                (LOWER(resource) LIKE '%backup%') OR
                (LOWER(resource) LIKE '%shell%') OR
                (LOWER(resource) LIKE '%upload%');

-- Group by IP address
C = GROUP B BY ip;

-- Count suspicious attempts per IP
D = FOREACH C GENERATE 
    group as suspicious_ip, 
    COUNT(B) as suspicious_attempts;

-- Filter for IPs with significant suspicious activity (>= 5 attempts)
E = FILTER D BY suspicious_attempts >= 5;

-- Sort by suspicious attempts in descending order
F = ORDER E BY suspicious_attempts DESC;

-- Display results
DUMP F;

-- Store results to HDFS
STORE E INTO '/user/hadoop/log_analysis/pig_output/suspicious_ips';
