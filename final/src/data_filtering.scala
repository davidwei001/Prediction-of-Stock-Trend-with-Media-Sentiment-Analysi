import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ IntegerType, FloatType }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{ LongWritable, Text }
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
// cluster mode for deploying
val spark = SparkSession.builder.master("local").appName("TweetSentiment").getOrCreate
import spark.implicits._
// read trademark.csv
val dfTrademark = spark.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "DROPMALFORMED").load("hdfs://localhost:1234/project/trademark_MSFT2.csv");

// read tweets.csv
// set delimiter
spark.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", "\n\n")
val rawTweets = spark.sparkContext.newAPIHadoopFile("hdfs://localhost:1234/project/tweets2009-06.txt", classOf[TextInputFormat], classOf[LongWritable], classOf[Text], spark.sparkContext.hadoopConfiguration)
// convert to dataframe <date, username, tweet>
val tweets = rawTweets.filter(s => (s._2.toString().split("\n").size == 3)).map(x => (x._2.toString().split("\n")(0).split("\t")(1).split(" ")(0).replaceAll("-", ""),x._2.toString().split("\n")(2).split("\t")(1).trim()))
var dfTweets = spark.createDataFrame(tweets)
dfTweets = dfTweets.withColumnRenamed("_1", "Date").withColumnRenamed("_2", "Tweet")
// merge trademark and tweets
dfTweets = dfTweets.crossJoin(dfTrademark).filter(col("Tweet").contains(col("trademark")))
// save as csv file
val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
val dirSentiment = "hdfs://localhost:1234/project/Sentiment"
if(fs.exists(new Path(dirSentiment)))  fs.delete(new Path(dirSentiment),true)
    
dfTweets.coalesce(1).write.option("header", "true").csv("hdfs://localhost:1234/project/Sentiment")