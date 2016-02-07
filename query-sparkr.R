#
# SparkR query of Hive table
# -------------------------------------

require(SparkR)

#sc <- sparkR.init(master = "local[*]")
sc <- sparkR.init(master = "yarn-client")

sqlContext <- sparkRHive.init(sc)

tableNames(sqlContext)

df <- sql(sqlContext, "select min(adate) as first, max(adate) as last from test_p1")

head(df)

df <- sql(sqlContext, "select year, min(adate) as first, max(adate) as last from test_p1 group by year")

head(df)

sparkR.stop()
