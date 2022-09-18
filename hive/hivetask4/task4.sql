ADD FILE ./mapper.py;

SET hive.auto.convert.join = false;

USE <db>;

SELECT TRANSFORM (ip, date_, http_, size_, code, client_app) USING './mapper.py' AS ip, date_, http_, size_, code, client_app FROM Logs LIMIT 10;

