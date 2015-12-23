/*
*
* YOU MUST RUN THIS FROM A LOCAL SPARK CONTEXT
*
*/

import java.sql.Timestamp
import org.joda.time._
import org.joda.time.format._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.types._

def createParquet(sqlContext: SQLContext, inPath: String, outPath: String): DataFrame = {

val pattern = "YYYY-mm-dd HH:mm:ss"

  val rowRdd = sqlContext.sparkContext.textFile(inPath).map { line =>
    val tokens = line.split('\t')
  
    val date = DateTime.parse(tokens(0), DateTimeFormat.forPattern(pattern))
    val year = tokens(1).stripPrefix("\"").stripSuffix("\"").toInt
    val month = tokens(2).stripPrefix("\"").stripSuffix("\"")
    val day = tokens(3).stripPrefix("\"").stripSuffix("\"").toInt
    val yearmonth = tokens(4).stripPrefix("\"").stripSuffix("\"").toInt
    val hour = tokens(5).stripPrefix("\"").stripSuffix("\"").toInt
    val minute = tokens(6).stripPrefix("\"").stripSuffix("\"").toInt
    val value = tokens(7).stripPrefix("\"").stripSuffix("\"")
    
    Row(new Timestamp(date.getMillis), year, month, day, yearmonth, hour, minute, value)
  }
  
  val fields = Seq(
    StructField("adate", TimestampType, true),
    StructField("year", IntegerType, true),
    StructField("month", StringType, true),
    StructField("day", IntegerType, true),
    StructField("yearmonth", IntegerType, true),
    StructField("hour", IntegerType, true),
    StructField("minute", IntegerType, true),
    StructField("value", StringType, true)
  )
  
  val schema = StructType(fields)
  val fullDF = sqlContext.createDataFrame(rowRdd, schema)
  
  fullDF.show
  
  fullDF.write.format("parquet").mode(SaveMode.Overwrite).save(outPath)
  
  fullDF
}

val fileBase = "file:///Z:/Users/vinnys/Documents/RProjects/spark-hive-poc/data"
val hdfsBase = "hdfs:///user/vinnys/data/pData"

// remove all files
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration

val fs =  FileSystem.get(new Configuration())

fs.delete(new Path(hdfsBase), true)
fs.mkdirs(new Path(hdfsBase))

// read/write 2010
val inPath = f"$fileBase/log-2010"
val outPath = f"$hdfsBase/year=2010"

createParquet(sqlContext, inPath, outPath)

// now read/write 2011
val inPath = f"$fileBase/log-2011"
val outPath = f"$hdfsBase/year=2011"

createParquet(sqlContext, inPath, outPath)

// now read/write 2012
val inPath = f"$fileBase/log-2012"
val outPath = f"$hdfsBase/year=2012"

createParquet(sqlContext, inPath, outPath)

// now read/write 2013
val inPath = f"$fileBase/log-2013"
val outPath = f"$hdfsBase/year=2013"

createParquet(sqlContext, inPath, outPath)


/*
PAUSE HERE...... 
- create external table in Hive
- test existanve in hive from there
*/
sqlContext.tableNames

sqlContext.sql("select min(adate) first, max(adate) last from test_p1").show

// now read/write 2014
val inPath = f"$fileBase/log-2014"
val outPath = f"$hdfsBase/year=2014"

createParquet(sqlContext, inPath, outPath)


// now read/write 2015
val inPath = f"$fileBase/log-2015"
val outPath = f"$hdfsBase/year=2015"

createParquet(sqlContext, inPath, outPath)

// now many more

for (i <- 1990 to 2010 ) {
  val inPath = f"$fileBase/log-$i"
  val outPath = f"$hdfsBase/year=$i"
  
  createParquet(sqlContext, inPath, outPath)
}

// final query over all the data
sqlContext.sql("select min(adate) first, max(adate) last from test_p1").show

// more queries
sqlContext.sql("select count(*) from test_p1").show

sqlContext.sql("select year, count(*) from test_p1 group by year").show(100)

