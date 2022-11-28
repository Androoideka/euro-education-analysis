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
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.functions.year
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

val natalityCodesColumnType = StructField("indic_de,geo\time", StringType, nullable = false)

val natalityYears = Range(2009, 2021, 1)
val natalityTimeColumns = natalityYears.map(year => year.toString())
val natalityTimeColumnTypes = natalityTimeColumns.map(column => StructField(column, StringType, nullable = false))

val natalityColumnTypes = natalityCodesColumnType +: natalityTimeColumnTypes
val natalitySchema = StructType(natalityColumnTypes)

val natalityInitialDF = spark.read.format("csv")
                  .schema(natalitySchema)
                  .option("delimiter", "\\t")
                  .option("header", true)
                  .load(natalityFile)
display(natalityInitialDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Clean data - remove unnecessary row and remove non-numeric characters from values in year columns.

// COMMAND ----------

val natalityRemoveLastDF = natalityInitialDF.filter(natalityInitialDF("indic_de,geo\time") =!= "GBIRTHRT_THSP")
var natalityNumbersDF = natalityRemoveLastDF

for(i <- natalityYears) {
  natalityNumbersDF = natalityNumbersDF.withColumn(i.toString + "number", regexp_replace(natalityNumbersDF(i.toString), ":", "0")).drop(i.toString)
  natalityNumbersDF = natalityNumbersDF.withColumn(i.toString + "number", regexp_replace(natalityNumbersDF(i.toString + "number"), "[^0-9]", ""))
  natalityNumbersDF = natalityNumbersDF.withColumn(i.toString, natalityNumbersDF(i.toString + "number").cast(IntegerType)).drop(i.toString + "number")
}

val natalityCleanDF = natalityNumbersDF
display(natalityCleanDF)

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
val natalityMap = getCodeMap(natalityStructureFile, "en")

// COMMAND ----------

// MAGIC %md
// MAGIC Translate country codes to country names in birth rate dataset.

// COMMAND ----------

var natalityTranslatedDF = natalityCleanDF.withColumnRenamed("indic_de,geo\time", "Country")

for ((code, description) <- natalityMap) {
  natalityTranslatedDF = natalityTranslatedDF.withColumn("Country", regexp_replace(natalityTranslatedDF("Country"), "^GBIRTHRT_THSP," + code + "$", description))
}

display(natalityTranslatedDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Clean once more - shorten EU grouping descriptions.

// COMMAND ----------

val euroDescriptions= Array("Germany (until 1990 former territory of the FRG)", "European Union - 15 countries (1995-2004)", "Euro area - 18 countries (2014)", "Euro area - 19 countries  (from 2015)", "European Union - 27 countries (2007-2013)", "European Union - 27 countries (from 2020)", "European Union - 28 countries (2013-2020)")

val euroTerms = Array("Germany", "EU15", "EU18", "EU19", "Old_EU27", "EU27", "EU28")

val euroMap = euroDescriptions zip euroTerms

var natalityOpaqueDF = natalityTranslatedDF
for ((original, sub) <- euroMap) {
  natalityOpaqueDF = natalityOpaqueDF.withColumn("Country", regexp_replace(natalityOpaqueDF("Country"), "\\Q" + original + "\\E", sub))
}
val natalityShortDF = natalityOpaqueDF

display(natalityShortDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Separate measurements by year.

// COMMAND ----------

var natalityMapDF = natalityShortDF.withColumn("year_map", map(natalityYears.map(year => lit(year.toString) :: natalityShortDF(year.toString) :: Nil).flatten: _*))

for (i <- natalityYears) {
  natalityMapDF = natalityMapDF.drop(i.toString)
}

natalityMapDF = natalityMapDF.select(natalityMapDF("*"), explode(natalityMapDF("year_map")))
natalityMapDF = natalityMapDF.withColumnRenamed("key", "Year").withColumnRenamed("value", "Births").drop("year_map")
natalityMapDF = natalityMapDF.withColumn("Year", natalityMapDF("Year").cast(IntegerType))
natalityMapDF.cache()
display(natalityMapDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Check education attainment dataset (from https://data.europa.eu/data/datasets/rwtzczizukv1zv5bhatyg?locale=en) and identify columns.

// COMMAND ----------

val educationFile = "dbfs:/FileStore/edat_lfs_9911.tsv"
dbutils.fs.head(educationFile)

// COMMAND ----------

// MAGIC %md
// MAGIC Create schema and import dataset into Spark. (education attainment)

// COMMAND ----------

val educationCodesColumnType = StructField("unit,sex,isced11,citizen,age,geo\time", StringType, nullable = false)

val educationYears = Range(2004, 2021, 1)
val educationTimeColumns = educationYears.map(year => year.toString())
val educationTimeColumnTypes = educationTimeColumns.map(column => StructField(column, StringType, nullable = false))

val educationColumnTypes = educationCodesColumnType +: educationTimeColumnTypes
val educationSchema = StructType(educationColumnTypes)

val educationInitialDF = spark.read.format("csv")
                  .schema(educationSchema)
                  .option("delimiter", "\\t")
                  .option("header", true)
                  .load(educationFile)
display(educationInitialDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Clean data - remove non-numeric characters from values in year columns and make an array out of codes.

// COMMAND ----------

val educationSplitDF = educationInitialDF.withColumn("codes", split(educationInitialDF("unit,sex,isced11,citizen,age,geo\time"), ",")).drop("unit,sex,isced11,citizen,age,geo\time")
var educationNumbersDF = educationSplitDF

for(i <- educationYears) {
  educationNumbersDF = educationNumbersDF.withColumn(i.toString + "number", regexp_replace(educationNumbersDF(i.toString), ":", "0")).drop(i.toString)
  educationNumbersDF = educationNumbersDF.withColumn(i.toString + "number", regexp_replace(educationNumbersDF(i.toString + "number"), raw"[^0-9\s.]+|\.(?!\d)", ""))
  educationNumbersDF = educationNumbersDF.withColumn(i.toString, educationNumbersDF(i.toString + "number").cast(DoubleType)).drop(i.toString + "number")
}

val educationCleanDF = educationNumbersDF
display(educationCleanDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Split codes into separate columns.

// COMMAND ----------

// MAGIC %md
// MAGIC First, assign a unique identifier to each column with a different combination of codes.

// COMMAND ----------

val genericWindow = Window.orderBy(lit(1))
val educationRowDF = educationCleanDF.withColumn("row_id", row_number().over(genericWindow))
display(educationRowDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Second, explode the array of codes while retaining information about where the code was in the array (posexplode). Form temporary column names for each code type. Then pivot.

// COMMAND ----------

val educationDesignationDF = educationRowDF.select(educationRowDF("row_id").alias("id"), educationRowDF("codes"), posexplode(educationRowDF("codes"))).drop("col")
val translatedCodesDF = educationDesignationDF.select(educationDesignationDF("id"),
                                                      concat(lit("code"), educationDesignationDF("pos").cast("string"))
                                                             .alias("col_names"),
                                                     expr("codes[pos]").alias("code"))
val pivotedCodesDF = translatedCodesDF.groupBy("id").pivot("col_names").agg(first("code"))
display(pivotedCodesDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Lastly, join the newly formed dataframe with separate columns to the old dataframe using the unique identifier. Rename columns to reflect code type.

// COMMAND ----------

val educationCodesDF = educationRowDF.join(pivotedCodesDF, educationRowDF("row_id") === pivotedCodesDF("id"), "inner")
val educationSeparateDF = educationCodesDF.drop(educationCodesDF("codes")).drop(educationCodesDF("id"))
// unit,sex,isced11,citizen,age,geo
val educationRenamedDF = educationSeparateDF.withColumnRenamed("code0", "Unit").withColumnRenamed("code1", "Sex").withColumnRenamed("code2", "ISCED11").withColumnRenamed("code3", "Citizen")
                        .withColumnRenamed("code4", "Age").withColumnRenamed("code5", "Country").drop("row_id")
display(educationRenamedDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Extract codes from EU education attainment XML structure file and map them to descriptions in English.

// COMMAND ----------

val educationStructureFile = "dbfs:/FileStore/edat_lfs_9911_dsd.xml"
val educationMap = getCodeMap(educationStructureFile, "en")

// COMMAND ----------

// MAGIC %md
// MAGIC Translate codes to descriptions in education attainment dataset.

// COMMAND ----------

val codeNames = Array("Unit", "Sex", "ISCED11", "Citizen", "Age", "Country")
var educationTranslatedDF = educationRenamedDF

for ((code, description) <- educationMap) {
  for (codeColumn <- codeNames) {
    educationTranslatedDF = educationTranslatedDF.withColumn(codeColumn, regexp_replace(educationTranslatedDF(codeColumn), "^" + code + "$", description))
  }
}

display(educationTranslatedDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Check for rogue values.

// COMMAND ----------

for (codeColumn <- codeNames) {
  educationTranslatedDF.select(codeColumn).distinct().show(false)
}

// COMMAND ----------

// MAGIC %md
// MAGIC Clean once more - remove unnecessary code columns and shorten others.

// COMMAND ----------

/*val educationAttainment = Array("Less than primary, primary and lower secondary education (levels 0-2)", 
                               "Upper secondary, post-secondary non-tertiary and tertiary education (levels 3-8)",
                               "Upper secondary and post-secondary non-tertiary education (levels 3 and 4)")

val ages = Array("From 15 to 24 years", "From 15 to 64 years", "From 15 to 69 years", "From 15 to 74 years", "From 18 to 24 years", "From 18 to 64 years", "From 18 to 69 years",
                "From 18 to 74 years", "From 20 to 24 years", "From 25 to 34 years", "From 25 to 54 years", "From 25 to 64 years", "From 25 to 69 years", "From 25 to 74 years",
                "From 30 to 34 years", "From 55 to 74 years")*/

val educationAttainment = educationTranslatedDF.select("ISCED11").distinct().collect().map(row => row(0).toString)
val ages = educationTranslatedDF.select("Age").distinct().collect().map(row => row(0).toString)

val educationAttainmentSub = Array("Secondary", "Tertiary", "Vocational")
val agesSub = Array("15-24", "15-64", "15-69", "15-74", "18-24", "18-64", "18-69", "18-74", "20-24", "25-34", "25-54", "25-64", "25-69", "25-74", "30-34", "55-74")

val educationAttainmentMap = educationAttainment zip educationAttainmentSub
val agesMap = ages zip agesSub

var educationOpaqueDF = educationTranslatedDF.drop("Unit").drop("Sex").filter(educationTranslatedDF("Citizen") === "Total").drop("Citizen").withColumnRenamed("ISCED11", "Education_Attainment")
for((original, sub) <- euroMap) {
  educationOpaqueDF = educationOpaqueDF.withColumn("Country", regexp_replace(educationOpaqueDF("Country"), "\\Q" + original + "\\E", sub))
}
for((original, sub) <- educationAttainmentMap) {
  educationOpaqueDF = educationOpaqueDF.withColumn("Education_Attainment", regexp_replace(educationOpaqueDF("Education_Attainment"), "\\Q" + original + "\\E", sub))
}
for((original, sub) <- agesMap) {
  educationOpaqueDF = educationOpaqueDF.withColumn("Age", regexp_replace(educationOpaqueDF("Age"), original, sub))
}
val educationShortDF = educationOpaqueDF

display(educationShortDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Separate measurements by year.

// COMMAND ----------

var educationMapDF = educationShortDF.withColumn("year_map", map(educationYears.map(year => lit(year.toString) :: educationShortDF(year.toString) :: Nil).flatten: _*))

for (i <- educationYears) {
  educationMapDF = educationMapDF.drop(i.toString)
}

educationMapDF = educationMapDF.select(educationMapDF("*"), explode(educationMapDF("year_map")))
educationMapDF = educationMapDF.withColumnRenamed("key", "Year").withColumnRenamed("value", "Proportion").drop("year_map")
educationMapDF = educationMapDF.withColumn("Year", educationMapDF("Year").cast(IntegerType))
educationMapDF.cache()

display(educationMapDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Move education attainment proportions to columns and separate into younger, middle-aged and older proportions.

// COMMAND ----------

val educationGlobalDF = educationMapDF.filter(educationMapDF("Age") === "18-74")
.groupBy("Country", "Year").pivot("Education_Attainment").sum("Proportion")

val educationYoungDF = educationMapDF.filter(educationMapDF("Age") === "18-24" || educationMapDF("Age") === "25-34")
.groupBy("Country", "Year").pivot("Education_Attainment").sum("Proportion")

val educationMiddleDF = educationMapDF.filter(educationMapDF("Age") === "25-54")
.groupBy("Country", "Year").pivot("Education_Attainment").sum("Proportion")

val educationOldDF = educationMapDF.filter(educationMapDF("Age") === "55-74")
.groupBy("Country", "Year").pivot("Education_Attainment").sum("Proportion")

display(educationGlobalDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Join datasets.

// COMMAND ----------

val joinedCols = List("Country", "Year")
val globalJoinedDF = natalityMapDF.join(educationGlobalDF, joinedCols)
val youngJoinedDF = natalityMapDF.join(educationYoungDF, joinedCols)
val middleJoinedDF = natalityMapDF.join(educationMiddleDF, joinedCols)
val oldJoinedDF = natalityMapDF.join(educationOldDF, joinedCols)
display (globalJoinedDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Save data to files.

// COMMAND ----------

val data_path = "dbfs:/FileStore/temp/analysis/"
globalJoinedDF.write.option("header", true).mode("overwrite").csv(data_path + "joined-global")
youngJoinedDF.write.option("header", true).mode("overwrite").csv(data_path + "joined-young")
middleJoinedDF.write.option("header", true).mode("overwrite").csv(data_path + "joined-middle-age")
oldJoinedDF.write.option("header", true).mode("overwrite").csv(data_path + "joined-old")
natalityMapDF.write.option("header", true).mode("overwrite").csv(data_path + "natality")
educationGlobalDF.write.option("header", true).mode("overwrite").csv(data_path + "education-global")
educationYoungDF.write.option("header", true).mode("overwrite").csv(data_path + "education-young")
educationMiddleDF.write.option("header", true).mode("overwrite").csv(data_path + "education-middle-age")
educationOldDF.write.option("header", true).mode("overwrite").csv(data_path + "education-old")

// COMMAND ----------

// MAGIC %md
// MAGIC Repartition education dataset for streaming.

// COMMAND ----------

val stream_path = "dbfs:/FileStore/temp/stream"
val educationStringDF = educationMapDF.withColumn("Year", educationMapDF("Year").cast(StringType))
val educationYearDF = educationStringDF.withColumn("Year", to_timestamp(educationStringDF("Year").cast(TimestampType)))

educationYearDF.write.partitionBy("Year").option("header", true).mode("overwrite").csv(stream_path)

display(educationYearDF)