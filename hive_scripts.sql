
-- HIVE SCRIPTS FOR LOG DATA ANALYSIS
-- SETUP: Create Hive Table
-- Run this first to create the table structure
CREATE TABLE web_logs (
    ip STRING,
    user STRING,
    timestamp STRING,
    method STRING,
    resource STRING,
    protocol STRING,
    status INT,
    size INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Load data into Hive table
LOAD DATA INPATH '/user/hadoop/log_analysis/input/extended_server_logs.csv' 
INTO TABLE web_logs;

-- =================================================================
-- TASK 1: MOST VISITED PAGES
-- =================================================================
-- Find which pages receive the most traffic (HTTP 200 successful requests)

SELECT resource, COUNT(*) as visits 
FROM web_logs 
WHERE status = 200 
GROUP BY resource 
ORDER BY visits DESC;

-- Expected Output:
-- /index.html       5
-- /products.html    4
-- /blog.html        3
-- /services.html    3
-- /about.html       2
-- /contact.html     2

-- =================================================================
-- TASK 2: PEAK TRAFFIC HOURS
-- =================================================================
-- Identify which hours have the most web traffic

-- Method 1: Simple hour extraction
SELECT SUBSTR(timestamp, 12, 2) as hour, COUNT(*) as requests 
FROM web_logs 
GROUP BY SUBSTR(timestamp, 12, 2) 
ORDER BY requests DESC;

-- Method 2: More detailed analysis with status breakdown
SELECT 
    SUBSTR(timestamp, 12, 2) as hour,
    COUNT(*) as total_requests,
    SUM(CASE WHEN status = 200 THEN 1 ELSE 0 END) as successful,
    SUM(CASE WHEN status = 403 THEN 1 ELSE 0 END) as forbidden,
    SUM(CASE WHEN status = 201 THEN 1 ELSE 0 END) as created
FROM web_logs 
GROUP BY SUBSTR(timestamp, 12, 2) 
ORDER BY total_requests DESC;

-- Expected Output:
-- Hour 12: 8 requests (PEAK)
-- Hour 13: 7 requests
-- Hour 10: 6 requests
-- Hour 11: 6 requests
-- Hour 14: 6 requests

-- TASK 3: SUSPICIOUS IP ACTIVITY

-- Detect IPs attempting suspicious activities (403 errors, admin access, etc)

-- Method 1: Basic suspicious IP detection
SELECT ip, COUNT(*) as suspicious_attempts 
FROM web_logs 
WHERE status = 403 OR 
      LOWER(resource) LIKE '%admin%' OR 
      LOWER(resource) LIKE '%config%' OR
      LOWER(resource) LIKE '%password%' OR
      LOWER(resource) LIKE '%passwd%'
GROUP BY ip 
HAVING COUNT(*) > 3
ORDER BY suspicious_attempts DESC;

-- Expected Output:
-- 203.0.113.45    16 (ATTACKER!)
-- 198.51.100.99   0  (Bot - no suspicious activity)

-- Method 2: Advanced analysis with detailed metrics
SELECT 
    ip, 
    COUNT(*) as total_attempts,
    COUNT(DISTINCT resource) as unique_resources_targeted,
    SUM(CASE WHEN status = 403 THEN 1 ELSE 0 END) as blocked_attempts,
    SUM(CASE WHEN status = 200 THEN 1 ELSE 0 END) as successful_attempts,
    ROUND(100.0 * SUM(CASE WHEN status = 403 THEN 1 ELSE 0 END) / COUNT(*), 2) as blocked_percentage
FROM web_logs 
GROUP BY ip
HAVING COUNT(*) > 5
ORDER BY blocked_percentage DESC;

-- Expected Output:
-- 203.0.113.45    16    16    16    0    100.00%    (ATTACKER - All blocked!)
-- 198.51.100.99   10    3     0     10   0.00%      (Bot - Legitimate)
-- 192.168.1.1     5     3     0     5    0.00%      (Legitimate user)


-- HTTP Status Code Distribution
SELECT status, COUNT(*) as count 
FROM web_logs 
GROUP BY status 
ORDER BY count DESC;

-- Top IPs by Traffic Volume
SELECT ip, COUNT(*) as total_requests 
FROM web_logs 
GROUP BY ip 
ORDER BY total_requests DESC 
LIMIT 10;

-- Request Size Analysis
SELECT 
    resource,
    COUNT(*) as requests,
    MIN(size) as min_size,
    ROUND(AVG(size), 2) as avg_size,
    MAX(size) as max_size
FROM web_logs 
WHERE status = 200
GROUP BY resource
ORDER BY avg_size DESC;

-- Temporal Analysis - Requests per Hour with Status Breakdown
SELECT 
    SUBSTR(timestamp, 1, 13) as hour_block,
    COUNT(*) as total_requests,
    SUM(CASE WHEN status = 200 THEN 1 ELSE 0 END) as successful,
    SUM(CASE WHEN status = 403 THEN 1 ELSE 0 END) as forbidden,
    SUM(CASE WHEN status = 201 THEN 1 ELSE 0 END) as created
FROM web_logs 
GROUP BY SUBSTR(timestamp, 1, 13)
ORDER BY hour_block;

-- Detect Attack Patterns by IP
SELECT 
    ip,
    COUNT(*) as total_requests,
    COUNT(DISTINCT resource) as unique_resources,
    SUM(CASE WHEN status = 403 THEN 1 ELSE 0 END) as blocked_attempts,
    ROUND(100.0 * SUM(CASE WHEN status = 403 THEN 1 ELSE 0 END) / COUNT(*), 2) as blocked_percentage,
    CASE 
        WHEN ROUND(100.0 * SUM(CASE WHEN status = 403 THEN 1 ELSE 0 END) / COUNT(*), 2) >= 90 THEN 'ATTACKER'
        WHEN COUNT(*) > 8 THEN 'SUSPICIOUS'
        ELSE 'LEGITIMATE'
    END as classification
FROM web_logs 
GROUP BY ip
ORDER BY blocked_percentage DESC;

