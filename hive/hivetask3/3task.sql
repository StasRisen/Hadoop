USE <DB>; 

SET hive.auto.convert.join=false; 
SET mapreduce.job.reduces=10;

SELECT 
	reg.region,
       	SUM(CASE WHEN usr.sex = "male" THEN 1 else 0 end) as men,
       	SUM(CASE WHEN usr.sex = "female" THEN 1 else 0 end) as women
FROM Logs l

INNER JOIN IPRegions reg ON l.ip = reg.ip
INNER JOIN Users usr ON l.ip = usr.ip

GROUP BY reg.region;
