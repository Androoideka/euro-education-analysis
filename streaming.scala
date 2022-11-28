// Databricks notebook source
// MAGIC %fs ls "dbfs:/FileStore/temp/stream"

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.functions.year
import org.apache.spark.sql.expressions.Window
import java.time._
import scala.xml.XML

// COMMAND ----------

val stream_path = "dbfs:/FileStore/temp/stream"

val countryColumnType = StructField("Country", StringType)
val yearColumnType = StructField("Year", TimestampType)
val educationColumnType = StructField("Education_Attainment", StringType)
val proportionColumnType = StructField("Proportion", DoubleType)
val ageColumnType = StructField("Age", StringType)

val schema = StructType(Array(countryColumnType, yearColumnType, educationColumnType, proportionColumnType))

// COMMAND ----------

var normalDF = spark.read.format("csv").option("header", true).csv(stream_path)
normalDF = normalDF.filter(year(normalDF("Year")) > 2004)
display(normalDF)

// COMMAND ----------

var streamingDF = spark.readStream
.schema(schema)
.option("maxFilesPerTrigger", 1)
.option("header", "true")
.csv(stream_path)

streamingDF = streamingDF
.filter(year(streamingDF("Year")) > 2004)
.groupBy(streamingDF("Country"), window(to_timestamp(streamingDF("Year")), "5 weeks"))
.max("Proportion")

val query = streamingDF.writeStream.format("memory").queryName("counts").outputMode("complete").start()

// COMMAND ----------

Thread.sleep(40000)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT *
// MAGIC FROM counts

// COMMAND ----------

Thread.sleep(20000)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT *
// MAGIC FROM counts

// COMMAND ----------

Thread.sleep(20000)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT *
// MAGIC FROM counts