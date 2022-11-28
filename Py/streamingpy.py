# Databricks notebook source
# MAGIC %fs ls "dbfs:/FileStore/temp/stream"

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.types._
# MAGIC import org.apache.spark.sql.functions.window
# MAGIC import org.apache.spark.sql.functions.to_timestamp
# MAGIC import org.apache.spark.sql.functions.year
# MAGIC import org.apache.spark.sql.expressions.Window
# MAGIC import java.time._
# MAGIC import scala.xml.XML

# COMMAND ----------

# MAGIC %scala
# MAGIC val stream_path = "dbfs:/FileStore/temp/stream"
# MAGIC 
# MAGIC val countryColumnType = StructField("Country", StringType)
# MAGIC val yearColumnType = StructField("Year", TimestampType)
# MAGIC val educationColumnType = StructField("Education_Attainment", StringType)
# MAGIC val proportionColumnType = StructField("Proportion", DoubleType)
# MAGIC val ageColumnType = StructField("Age", StringType)
# MAGIC 
# MAGIC val schema = StructType(Array(countryColumnType, yearColumnType, educationColumnType, proportionColumnType))

# COMMAND ----------

# MAGIC %scala
# MAGIC var normalDF = spark.read.format("csv").option("header", true).csv(stream_path)
# MAGIC normalDF = normalDF.filter(year(normalDF("Year")) > 2004)
# MAGIC display(normalDF)

# COMMAND ----------

# MAGIC %scala
# MAGIC var streamingDF = spark.readStream
# MAGIC .schema(schema)
# MAGIC .option("maxFilesPerTrigger", 1)
# MAGIC .option("header", "true")
# MAGIC .csv(stream_path)
# MAGIC 
# MAGIC streamingDF = streamingDF
# MAGIC .filter(year(streamingDF("Year")) > 2004)
# MAGIC .groupBy(streamingDF("Country"), window(to_timestamp(streamingDF("Year")), "5 weeks"))
# MAGIC .max("Proportion")
# MAGIC 
# MAGIC val query = streamingDF.writeStream.format("memory").queryName("counts").outputMode("complete").start()

# COMMAND ----------

# MAGIC %scala
# MAGIC Thread.sleep(30000)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM counts

# COMMAND ----------

# MAGIC %scala
# MAGIC Thread.sleep(10000)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM counts

# COMMAND ----------

# MAGIC %scala
# MAGIC Thread.sleep(10000)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM counts