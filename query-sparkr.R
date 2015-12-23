#
# SparkR query of Hive table
# -------------------------------------

require(SparkR)

sc <- sparkR.init(master = "local[*]")

sqlContext <- sparkRHive.init(sc)

tableNames(sqlContext)

df <- sql(sqlContext, "select min(adate) as first, max(adate) as last from test_p1")

head(df)

sparkR.stop()
