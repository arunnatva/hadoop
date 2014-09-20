# This scripts calls the LogParsing.pig script

#!/bin/bash
yesterday=`TZ=aaa24 date +%Y/%m/%d`
datedDir=`TZ=aaa24 date +%Y-%m-%d`
hadoop fs -rmr -skipTrash /user/aknatva/source
hadoop fs -rmr -skipTrash /user/aknatva/Staging
hadoop fs -rmr -skipTrash /user/aknatva/target
pig -param yday="$yesterday" /home/aknatva/LogParsing.pig
hadoop fs -cat /user/aknatva/target/* | hadoop fs -put - /user/aknatva/EventMetrics/${datedDir}
