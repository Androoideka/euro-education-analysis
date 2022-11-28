// Databricks notebook source
// MAGIC %fs ls "dbfs:/FileStore/"

// COMMAND ----------

// MAGIC %md
// MAGIC Imports

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.posexplode
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.functions.first
import org.apache.spark.sql.functions.map
import org.apache.spark.sql.expressions.Window
import scala.xml.XML

// COMMAND ----------

// MAGIC %md
// MAGIC Check birth rate dataset (from https://data.europa.eu/data/datasets/cbpeglqdhx6jfxjb8vog?locale=en) and identify columns.

// COMMAND ----------

val natalityFile = "dbfs:/FileStore/tps00204.tsv"
dbutils.fs.head(natalityFile)

// COMMAND ----------

// MAGIC %md
// MAGIC Create schema and import dataset into Spark. (birth rate)

// COMMAND ----------

val geoColumnType = StructField("indic_de,geo\time", StringType, nullable = false)

val natalityYears = Range(2009, 2021, 1)
val timeColumns = natalityYears.map(year => year.toString())
val timeColumnTypes = timeColumns.map(column => StructField(column, StringType, nullable = false))

val columnTypes = geoColumnType +: timeColumnTypes
val schema = StructType(columnTypes)

val natalityInitialDF = spark.read.format("csv")
                  .schema(schema)
                  .option("delimiter", "\\t")
                  .option("header", true)
                  .load(natalityFile)
display(natalityInitialDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Generic function for processing EU XML DSD files. These files describe the codes found in EU-related datasets. Descriptions are available in English, French and German.

// COMMAND ----------

def getCodeMap(structureFile: String, language: String): Map[String, String] = {
  val structure = XML.load(structureFile)
  // Loading XML nodes with codes which represent EU-related entities
  val codesXML = structure \\ "Code"
  // Loading descriptions of EU-related entities by accessing child nodes of above
  // Filter descriptions by language specified in arguments
  val entitiesXML = codesXML.map(code => (code \\ "Description").filter(description => description.attributes.value.toString == language))
  // Map XML nodes to value attribute which contains the code
  val codes = codesXML.map(code => code.attribute("value").get.toString)
  // Access description of XML nodes 
  val entities = entitiesXML.map(entity => entity.text)
  return (codes zip entities).toMap
}

// COMMAND ----------

// MAGIC %md
// MAGIC Extract codes from EU birth rate XML structure file and map them to descriptions in English. For this particular dataset, the only important codes are the country related ones.

// COMMAND ----------

val natalityStructureFile = "dbfs:/FileStore/tps00204_dsd.xml"
val countryMap = getCodeMap(natalityStructureFile, "en")

// COMMAND ----------

// MAGIC %md
// MAGIC Translate country codes to country names in birth rate dataset.

// COMMAND ----------

var natalityCountriesDF = natalityInitialDF.withColumn("Country", natalityInitialDF("indic_de,geo	ime"))
for ((code, country) <- countryMap) {
  natalityCountriesDF = natalityCountriesDF.withColumn("Country", regexp_replace(natalityCountriesDF("Country"), "^GBIRTHRT_THSP," + code + "$", country))
}
natalityCountriesDF = natalityCountriesDF.drop(natalityCountriesDF("indic_de,geo	ime"))

display(natalityCountriesDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Remove unnecessary row and split EU subtotals into their own dataframe.

// COMMAND ----------

val natalityRemoveLastDF = natalityCountriesDF.filter(natalityCountriesDF("Country") =!= "GBIRTHRT_THSP")
display(natalityRemoveLastDF)
val natalityNoEuro = natalityRemoveLastDF.filter(!natalityRemoveLastDF("Country").contains("Euro"))
val natalityOnlyEuro = natalityRemoveLastDF.filter(natalityRemoveLastDF("Country").contains("Euro"))
display(natalityNoEuro)

// COMMAND ----------

// MAGIC %md
// MAGIC Check education level data (from https://data.europa.eu/data/datasets/2rdgenqayvidkf7nfm2g?locale=en) and identify columns.

// COMMAND ----------

val educationFile = "dbfs:/FileStore/educ_uoe_enra03.tsv"
dbutils.fs.head(educationFile)

// COMMAND ----------

// MAGIC %md
// MAGIC Create schema and import dataset into Spark. (education level)

// COMMAND ----------

val geoColumnType = StructField("indic_ur,cities\time", StringType, nullable = false)

val educationYears = Range(2012, 2020, 1)
val timeColumns = educationYears.map(year => year.toString())
val timeColumnTypes = timeColumns.map(column => StructField(column, StringType, nullable = false))

val columnTypes = geoColumnType +: timeColumnTypes
val schema = StructType(columnTypes)

val educationInitialDF = spark.read.format("csv")
                  .schema(schema)
                  .option("delimiter", "\\t")
                  .option("header", true)
                  .load(educationFile)

// COMMAND ----------

// MAGIC %md
// MAGIC Extract codes from EU education level XML structure file and map them to descriptions in English.

// COMMAND ----------

val educationStructureFile = "dbfs:/FileStore/educ_uoe_enra03_dsd.xml"
val educationMap = getCodeMap(educationStructureFile, "en")

// COMMAND ----------

// MAGIC %md
// MAGIC All codes for a specific measurement are concatenated in a single column. Codes have to be separated into different columns so that the dataset can be grouped by country or education level. Preparation includes assigning a new identifier to each measurement and turning the concatenated codes into an array.

// COMMAND ----------

val splitCols = split(educationInitialDF("indic_ur,cities\time"), ",")
val educationSplitDF = educationInitialDF.withColumn("InfoArray", splitCols)
val genericWindow = Window.orderBy(lit(1))
val educationRowDF = educationSplitDF.withColumn("row_id", row_number().over(genericWindow))
display(educationRowDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Separate the codes into separate columns. We explode the array using posexplode so that information about a code's position in the array is retained. Then we use this retained position to create a column name and to fetch the value of the code from the array. Then we pivot the dataframe to turn the code and column name rows into columns.

// COMMAND ----------

val educationDesignationDF = educationRowDF.select(educationRowDF("row_id").alias("id"), educationRowDF("InfoArray"), posexplode(educationRowDF("InfoArray"))).drop("col")
val titledCodesDF = educationDesignationDF.select(educationDesignationDF("id"),
                                                      concat(lit("code"), educationDesignationDF("pos").cast("string"))
                                                             .alias("col_names"),
                                                     expr("InfoArray[pos]").alias("code"))
val pivotedCodesDF = titledCodesDF.groupBy("id").pivot("col_names").agg(first("code"))
display(pivotedCodesDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Merge the newly-created code columns with the rest of the education level dataset and remove unnecessary columns.

// COMMAND ----------

val educationCodesDF = educationRowDF.join(pivotedCodesDF, educationRowDF("row_id") === pivotedCodesDF("id"), "inner")
val educationCleanDF = educationCodesDF.drop("indic_ur,cities\time").drop(educationCodesDF("InfoArray")).drop(educationCodesDF("id"))
display(educationCleanDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Map codes to the descriptions provided by the XML file.

// COMMAND ----------

var educationTranslatedDF = educationCleanDF
for (i <- Range(0, 5, 1)) {
  for ((code, entity) <- educationMap) {
    val column = "code" + i.toString
    educationTranslatedDF = educationTranslatedDF.withColumn(column, regexp_replace(educationTranslatedDF(column), "^" + code + "$", entity))
  }
}
educationTranslatedDF.cache()

display(educationTranslatedDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Establish which information is needed. Code 2 specifies education level and code 4 specifies country (and EU subtotals).

// COMMAND ----------

val code0DF = educationTranslatedDF.select(educationTranslatedDF("code0")).distinct()
display(code0DF)

// COMMAND ----------

val code1DF = educationTranslatedDF.select(educationTranslatedDF("code1")).distinct()
display(code1DF)

// COMMAND ----------

val code2DF = educationTranslatedDF.select(educationTranslatedDF("code2")).distinct()
display(code2DF)

// COMMAND ----------

val code3DF = educationTranslatedDF.select(educationTranslatedDF("code3")).distinct()
display(code3DF)

// COMMAND ----------

val code4DF = educationTranslatedDF.select(educationTranslatedDF("code4")).distinct()
display(code4DF)

// COMMAND ----------

// MAGIC %md
// MAGIC Rename columns and drop unnecessary rows. Filter code 3 so that we only have totals and create a separate dataframe for EU subtotals.

// COMMAND ----------

val educationRenamedDF = educationTranslatedDF.withColumnRenamed("code4", "Country").withColumnRenamed("code2", "Education_Level").withColumnRenamed("code3", "Sex").withColumnRenamed("code1", "ISCEDF13").drop("code0")
val educationNoGenderDF = educationRenamedDF.filter(educationRenamedDF("Sex") =!= "Total").drop("Sex")

val educationNoEuroDF = educationNoGenderDF.filter(!educationNoGenderDF("Country").contains("Euro"))
val educationOnlyEuroDF = educationNoGenderDF.filter(educationNoGenderDF("Country").contains("Euro"))
display(educationNoEuroDF)

// COMMAND ----------

val educationYears = Range(2012, 2020, 1)
val educationYearDF = educationNoGenderDF.select(educationNoGenderDF("row_id"), map(educationYears.map(year => lit(year.toString) :: col(year.toString) :: Nil).flatten: _*).alias("year_map"))
val educationPartitionedDF = educationYearDF.select(educationYearDF("row_id"), explode(educationYearDF("year_map").alias("year", "num-")))
display(educationPartitionedDF)