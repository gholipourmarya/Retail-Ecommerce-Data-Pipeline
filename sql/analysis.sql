SELECT group_key, COUNT(*) AS records, ROUND(SUM(value), 2) AS total_value, SUM(flag) AS flagged_records
FROM events
GROUP BY group_key
ORDER BY total_value DESC;
