
// This pig script reads an unstructured dataset and parses it to convert it to a comma separated record
 // It uses a pig UDF written in java for parsing text
 
 REGISTER /home/aknatva/pig/UDFs/ParsingUDF.jar;
  DEFINE parseFunc com.arun.code.ParseFunction;
 
 RawFiles = LOAD '/user/aknatva/source/rawdata' USING PigStorage() as (line: chararray);
 
 // split the line record into 3 parts with double quotes as delimiter
  RawFilesStg1 = foreach RawFiles generate STRSPLIT(line, '"',3);
 
 //store it into a file
 store RawFilesStg1 into '/user/aknatva/Staging/stg1';
 
 //load into a variable for each line 3 fields
 RawFilesStg2 = load '/user/aknatva/Staging/stg1' using PigStorage(',') as (type:chararray,eventid:chararray, eventdetail:chararray);
 
 RawFilesStg3 = foreach RawFilesStg2 generate eventid,eventdetail;
 
 EventRecords = filter RawFilesStg3 by ($0 MATCHES 'event.*');
 
 SingleEventRecs = group EventRecords by $0;
 
 store SingleEventRecs into '/user/aknatva/Staging/stg2';
 
 EventDets = load '/user/aknatva/Staging/stg2' using PigStorage() as (eventid:chararray, record:chararray);
 
 MetricsFormat = foreach EventDets generate getMetrics(record);
 
 store MetricsFormat into '/user/aknatva/target/FormattedEvents';
 
