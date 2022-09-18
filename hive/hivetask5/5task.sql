USE <db>;

SET hive.auto.convert.join=false;
SET mapreduce.job.reduces=10;

SELECT 
	reg.region, 
	SUM(CASE WHEN usr.sex = "male" THEN 1 else 0 end) as men,
	SUM(CASE WHEN usr.sex = "female" THEN 1 else 0 end) as women
FROM Logs TABLESAMPLE(7065 ROWS) l
LEFT OUTER JOIN IPRegions TABLESAMPLE(6092 ROWS) reg ON l.ip = reg.ip
LEFT OUTER JOIN Users TABLESAMPLE(6015 ROWS) usr ON l.ip = usr.ip GROUP BY reg.region;
