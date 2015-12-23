#
# Generate a set of csv files
# 

dataDir <- file.path("data")

firstYear <- 1990
lastYear <- 2015

# create dataframe: adate | year | month | day | <this is the date> | <row number 1:100>

for ( year in seq(firstYear, lastYear, by=1) ) {
  outFile <- file.path(dataDir, sprintf("log-%d", year))
  
  startD <- as.POSIXct(sprintf("%d-01-01 00:00:00", year), tz="GMT")
  endD <- as.POSIXct(sprintf("%d-12-31 23:59:59", year), tz="GMT")
  
  d <- seq(from=startD, to=endD, by='min')
  
  df <- data.frame( date=d )
  
  df$year <- format(df$date, "%Y")
  df$month <- format(df$date, "%b")
  df$day <- format(df$date, "%d")
  df$yearmonth <- format(df$date, "%Y%m")
  df$hour <- format(df$date, "%H")
  df$minute <- format(df$date, "%M")
  
  df$value <- sprintf("%d hour of %d minute of %d of %s, in the year %s", as.integer(df$hour), as.integer(df$minute), as.integer( df$day ), df$month, df$year)
  
  write.table(x=df, file=outFile, sep='\t', row.names=FALSE, col.names=FALSE, append=FALSE)
}

# read the last file
fullDF <- read.csv(outFile, sep='\t', 
                   header=FALSE,
                   col.names=c("adate", "year", "month", "day", "yearmonth", "hour", "minute", "value"),
                   stringsAsFactor=FALSE)

nrow(fullDF)
summary(fullDF)

head(fullDF)
tail(fullDF)

