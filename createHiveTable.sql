#
# Create the External Hive Table using Parquet data
#

#
# be sure to have the column names match schema in parquet (what is selected from?)
#
# REPAIR TABLE finds the paritions
#

CREATE EXTERNAL TABLE test_p1
(
  adate timestamp COMMENT "event time", 
  month string COMMENT "Month name (short format)", 
  day int COMMENT "Day of Month", 
  yearmonth int COMMENT "YYYYMM for keying by year and month together", 
  hour int COMMENT "Hour of the event: 0-23", 
  minute int COMMENT "Minute of the event: 0-59", 
  value string COMMENT "Event String"
)
COMMENT "table for testing spark, partquet, hive integration"
PARTITIONED BY (year int COMMENT "year of the event")
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/user/vinnys/data/pData';

MSCK REPAIR TABLE test_p1;