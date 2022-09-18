#!/usr/bin/env bash
hive -e 'use <db>; SELECT date_, count(ip) as cnt FROM Logs GROUP BY date_ ORDER BY cnt DESC;'
