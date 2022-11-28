# Databricks notebook source
# MAGIC %fs ls "dbfs:/FileStore/"

# COMMAND ----------

# MAGIC %md
# MAGIC Imports

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.types._
# MAGIC import org.apache.spark.sql.functions.regexp_replace
# MAGIC import org.apache.spark.sql.functions.split
# MAGIC import org.apache.spark.sql.functions.explode
# MAGIC import org.apache.spark.sql.functions.posexplode
# MAGIC import org.apache.spark.sql.functions.lit
# MAGIC import org.apache.spark.sql.functions.expr
# MAGIC import org.apache.spark.sql.functions.concat
# MAGIC import org.apache.spark.sql.functions.row_number
# MAGIC import org.apache.spark.sql.functions.first
# MAGIC import org.apache.spark.sql.functions.map
# MAGIC import org.apache.spark.sql.functions.to_timestamp
# MAGIC import org.apache.spark.sql.functions.year
# MAGIC import org.apache.spark.sql.expressions.Window
# MAGIC import scala.xml.XML

# COMMAND ----------

# MAGIC %md
# MAGIC Check birth rate dataset (from https://data.europa.eu/data/datasets/cbpeglqdhx6jfxjb8vog?locale=en) and identify columns.

# COMMAND ----------

# MAGIC %scala
# MAGIC val natalityFile = "dbfs:/FileStore/tps00204.tsv"
# MAGIC dbutils.fs.head(natalityFile)

# COMMAND ----------

# MAGIC %md
# MAGIC Create schema and import dataset into Spark. (birth rate)

# COMMAND ----------

# MAGIC %scala
# MAGIC val natalityCodesColumnType = StructField("indic_de,geo\time", StringType, nullable = false)
# MAGIC 
# MAGIC val natalityYears = Range(2009, 2021, 1)
# MAGIC val natalityTimeColumns = natalityYears.map(year => year.toString())
# MAGIC val natalityTimeColumnTypes = natalityTimeColumns.map(column => StructField(column, StringType, nullable = false))
# MAGIC 
# MAGIC val natalityColumnTypes = natalityCodesColumnType +: natalityTimeColumnTypes
# MAGIC val natalitySchema = StructType(natalityColumnTypes)
# MAGIC 
# MAGIC val natalityInitialDF = spark.read.format("csv")
# MAGIC                   .schema(natalitySchema)
# MAGIC                   .option("delimiter", "\\t")
# MAGIC                   .option("header", true)
# MAGIC                   .load(natalityFile)
# MAGIC display(natalityInitialDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Clean data - remove unnecessary row and remove non-numeric characters from values in year columns.

# COMMAND ----------

# MAGIC %scala
# MAGIC val natalityRemoveLastDF = natalityInitialDF.filter(natalityInitialDF("indic_de,geo\time") =!= "GBIRTHRT_THSP")
# MAGIC var natalityNumbersDF = natalityRemoveLastDF
# MAGIC 
# MAGIC for(i <- natalityYears) {
# MAGIC   natalityNumbersDF = natalityNumbersDF.withColumn(i.toString + "number", regexp_replace(natalityNumbersDF(i.toString), ":", "0")).drop(i.toString)
# MAGIC   natalityNumbersDF = natalityNumbersDF.withColumn(i.toString + "number", regexp_replace(natalityNumbersDF(i.toString + "number"), "[^0-9]", ""))
# MAGIC   natalityNumbersDF = natalityNumbersDF.withColumn(i.toString, natalityNumbersDF(i.toString + "number").cast(IntegerType)).drop(i.toString + "number")
# MAGIC }
# MAGIC 
# MAGIC val natalityCleanDF = natalityNumbersDF
# MAGIC display(natalityCleanDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Generic function for processing EU XML DSD files. These files describe the codes found in EU-related datasets. Descriptions are available in English, French and German.

# COMMAND ----------

# MAGIC %scala
# MAGIC def getCodeMap(structureFile: String, language: String): Map[String, String] = {
# MAGIC   val structure = XML.load(structureFile)
# MAGIC   // Loading XML nodes with codes which represent EU-related entities
# MAGIC   val codesXML = structure \\ "Code"
# MAGIC   // Loading descriptions of EU-related entities by accessing child nodes of above
# MAGIC   // Filter descriptions by language specified in arguments
# MAGIC   val entitiesXML = codesXML.map(code => (code \\ "Description").filter(description => description.attributes.value.toString == language))
# MAGIC   // Map XML nodes to value attribute which contains the code
# MAGIC   val codes = codesXML.map(code => code.attribute("value").get.toString)
# MAGIC   // Access description of XML nodes 
# MAGIC   val entities = entitiesXML.map(entity => entity.text)
# MAGIC   return (codes zip entities).toMap
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC Extract codes from EU birth rate XML structure file and map them to descriptions in English. For this particular dataset, the only important codes are the country related ones.

# COMMAND ----------

# MAGIC %scala
# MAGIC val natalityStructureFile = "dbfs:/FileStore/tps00204_dsd.xml"
# MAGIC val natalityMap = getCodeMap(natalityStructureFile, "en")

# COMMAND ----------

# MAGIC %md
# MAGIC Translate country codes to country names in birth rate dataset.

# COMMAND ----------

# MAGIC %scala
# MAGIC var natalityTranslatedDF = natalityCleanDF.withColumnRenamed("indic_de,geo\time", "Country")
# MAGIC 
# MAGIC for ((code, description) <- natalityMap) {
# MAGIC   natalityTranslatedDF = natalityTranslatedDF.withColumn("Country", regexp_replace(natalityTranslatedDF("Country"), "^GBIRTHRT_THSP," + code + "$", description))
# MAGIC }
# MAGIC 
# MAGIC display(natalityTranslatedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Clean once more - shorten EU grouping descriptions.

# COMMAND ----------

# MAGIC %scala
# MAGIC val euroDescriptions= Array("Germany (until 1990 former territory of the FRG)", "European Union - 15 countries (1995-2004)", "Euro area - 18 countries (2014)", "Euro area - 19 countries  (from 2015)", "European Union - 27 countries (2007-2013)", "European Union - 27 countries (from 2020)", "European Union - 28 countries (2013-2020)")
# MAGIC 
# MAGIC val euroTerms = Array("Germany", "EU15", "EU18", "EU19", "Old_EU27", "EU27", "EU28")
# MAGIC 
# MAGIC val euroMap = euroDescriptions zip euroTerms
# MAGIC 
# MAGIC var natalityOpaqueDF = natalityTranslatedDF
# MAGIC for ((original, sub) <- euroMap) {
# MAGIC   natalityOpaqueDF = natalityOpaqueDF.withColumn("Country", regexp_replace(natalityOpaqueDF("Country"), "\\Q" + original + "\\E", sub))
# MAGIC }
# MAGIC val natalityShortDF = natalityOpaqueDF
# MAGIC 
# MAGIC display(natalityShortDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Separate measurements by year.

# COMMAND ----------

# MAGIC %scala
# MAGIC var natalityMapDF = natalityShortDF.withColumn("year_map", map(natalityYears.map(year => lit(year.toString) :: natalityShortDF(year.toString) :: Nil).flatten: _*))
# MAGIC 
# MAGIC for (i <- natalityYears) {
# MAGIC   natalityMapDF = natalityMapDF.drop(i.toString)
# MAGIC }
# MAGIC 
# MAGIC natalityMapDF = natalityMapDF.select(natalityMapDF("*"), explode(natalityMapDF("year_map")))
# MAGIC natalityMapDF = natalityMapDF.withColumnRenamed("key", "Year").withColumnRenamed("value", "Births").drop("year_map")
# MAGIC natalityMapDF = natalityMapDF.withColumn("Year", natalityMapDF("Year").cast(IntegerType))
# MAGIC natalityMapDF.cache()
# MAGIC display(natalityMapDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Check education attainment dataset (from https://data.europa.eu/data/datasets/rwtzczizukv1zv5bhatyg?locale=en) and identify columns.

# COMMAND ----------

# MAGIC %scala
# MAGIC val educationFile = "dbfs:/FileStore/edat_lfs_9911.tsv"
# MAGIC dbutils.fs.head(educationFile)

# COMMAND ----------

# MAGIC %md
# MAGIC Create schema and import dataset into Spark. (education attainment)

# COMMAND ----------

# MAGIC %scala
# MAGIC val educationCodesColumnType = StructField("unit,sex,isced11,citizen,age,geo\time", StringType, nullable = false)
# MAGIC 
# MAGIC val educationYears = Range(2004, 2021, 1)
# MAGIC val educationTimeColumns = educationYears.map(year => year.toString())
# MAGIC val educationTimeColumnTypes = educationTimeColumns.map(column => StructField(column, StringType, nullable = false))
# MAGIC 
# MAGIC val educationColumnTypes = educationCodesColumnType +: educationTimeColumnTypes
# MAGIC val educationSchema = StructType(educationColumnTypes)
# MAGIC 
# MAGIC val educationInitialDF = spark.read.format("csv")
# MAGIC                   .schema(educationSchema)
# MAGIC                   .option("delimiter", "\\t")
# MAGIC                   .option("header", true)
# MAGIC                   .load(educationFile)
# MAGIC display(educationInitialDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Clean data - remove non-numeric characters from values in year columns and make an array out of codes.

# COMMAND ----------

# MAGIC %scala
# MAGIC val educationSplitDF = educationInitialDF.withColumn("codes", split(educationInitialDF("unit,sex,isced11,citizen,age,geo\time"), ",")).drop("unit,sex,isced11,citizen,age,geo\time")
# MAGIC var educationNumbersDF = educationSplitDF
# MAGIC 
# MAGIC for(i <- educationYears) {
# MAGIC   educationNumbersDF = educationNumbersDF.withColumn(i.toString + "number", regexp_replace(educationNumbersDF(i.toString), ":", "0")).drop(i.toString)
# MAGIC   educationNumbersDF = educationNumbersDF.withColumn(i.toString + "number", regexp_replace(educationNumbersDF(i.toString + "number"), raw"[^0-9\s.]+|\.(?!\d)", ""))
# MAGIC   educationNumbersDF = educationNumbersDF.withColumn(i.toString, educationNumbersDF(i.toString + "number").cast(DoubleType)).drop(i.toString + "number")
# MAGIC }
# MAGIC 
# MAGIC val educationCleanDF = educationNumbersDF
# MAGIC display(educationCleanDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Split codes into separate columns.

# COMMAND ----------

# MAGIC %md
# MAGIC First, assign a unique identifier to each column with a different combination of codes.

# COMMAND ----------

# MAGIC %scala
# MAGIC val genericWindow = Window.orderBy(lit(1))
# MAGIC val educationRowDF = educationCleanDF.withColumn("row_id", row_number().over(genericWindow))
# MAGIC display(educationRowDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Second, explode the array of codes while retaining information about where the code was in the array (posexplode). Form temporary column names for each code type. Then pivot.

# COMMAND ----------

# MAGIC %scala
# MAGIC val educationDesignationDF = educationRowDF.select(educationRowDF("row_id").alias("id"), educationRowDF("codes"), posexplode(educationRowDF("codes"))).drop("col")
# MAGIC val translatedCodesDF = educationDesignationDF.select(educationDesignationDF("id"),
# MAGIC                                                       concat(lit("code"), educationDesignationDF("pos").cast("string"))
# MAGIC                                                              .alias("col_names"),
# MAGIC                                                      expr("codes[pos]").alias("code"))
# MAGIC val pivotedCodesDF = translatedCodesDF.groupBy("id").pivot("col_names").agg(first("code"))
# MAGIC display(pivotedCodesDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Lastly, join the newly formed dataframe with separate columns to the old dataframe using the unique identifier. Rename columns to reflect code type.

# COMMAND ----------

# MAGIC %scala
# MAGIC val educationCodesDF = educationRowDF.join(pivotedCodesDF, educationRowDF("row_id") === pivotedCodesDF("id"), "inner")
# MAGIC val educationSeparateDF = educationCodesDF.drop(educationCodesDF("codes")).drop(educationCodesDF("id"))
# MAGIC // unit,sex,isced11,citizen,age,geo
# MAGIC val educationRenamedDF = educationSeparateDF.withColumnRenamed("code0", "Unit").withColumnRenamed("code1", "Sex").withColumnRenamed("code2", "ISCED11").withColumnRenamed("code3", "Citizen")
# MAGIC                         .withColumnRenamed("code4", "Age").withColumnRenamed("code5", "Country").drop("row_id")
# MAGIC display(educationRenamedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Extract codes from EU education attainment XML structure file and map them to descriptions in English.

# COMMAND ----------

# MAGIC %scala
# MAGIC val educationStructureFile = "dbfs:/FileStore/edat_lfs_9911_dsd.xml"
# MAGIC val educationMap = getCodeMap(educationStructureFile, "en")

# COMMAND ----------

# MAGIC %md
# MAGIC Translate codes to descriptions in education attainment dataset.

# COMMAND ----------

# MAGIC %scala
# MAGIC val codeNames = Array("Unit", "Sex", "ISCED11", "Citizen", "Age", "Country")
# MAGIC var educationTranslatedDF = educationRenamedDF
# MAGIC 
# MAGIC for ((code, description) <- educationMap) {
# MAGIC   for (codeColumn <- codeNames) {
# MAGIC     educationTranslatedDF = educationTranslatedDF.withColumn(codeColumn, regexp_replace(educationTranslatedDF(codeColumn), "^" + code + "$", description))
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC display(educationTranslatedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Check for rogue values.

# COMMAND ----------

# MAGIC %scala
# MAGIC for (codeColumn <- codeNames) {
# MAGIC   educationTranslatedDF.select(codeColumn).distinct().show(false)
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC Clean once more - remove unnecessary code columns and shorten others.

# COMMAND ----------

# MAGIC %scala
# MAGIC /*val educationAttainment = Array("Less than primary, primary and lower secondary education (levels 0-2)", 
# MAGIC                                "Upper secondary, post-secondary non-tertiary and tertiary education (levels 3-8)",
# MAGIC                                "Upper secondary and post-secondary non-tertiary education (levels 3 and 4)")
# MAGIC 
# MAGIC val ages = Array("From 15 to 24 years", "From 15 to 64 years", "From 15 to 69 years", "From 15 to 74 years", "From 18 to 24 years", "From 18 to 64 years", "From 18 to 69 years",
# MAGIC                 "From 18 to 74 years", "From 20 to 24 years", "From 25 to 34 years", "From 25 to 54 years", "From 25 to 64 years", "From 25 to 69 years", "From 25 to 74 years",
# MAGIC                 "From 30 to 34 years", "From 55 to 74 years")*/
# MAGIC 
# MAGIC val educationAttainment = educationTranslatedDF.select("ISCED11").distinct().collect().map(row => row(0).toString)
# MAGIC val ages = educationTranslatedDF.select("Age").distinct().collect().map(row => row(0).toString)
# MAGIC 
# MAGIC val educationAttainmentSub = Array("Secondary", "Tertiary", "Vocational")
# MAGIC val agesSub = Array("15-24", "15-64", "15-69", "15-74", "18-24", "18-64", "18-69", "18-74", "20-24", "25-34", "25-54", "25-64", "25-69", "25-74", "30-34", "55-74")
# MAGIC 
# MAGIC val educationAttainmentMap = educationAttainment zip educationAttainmentSub
# MAGIC val agesMap = ages zip agesSub
# MAGIC 
# MAGIC var educationOpaqueDF = educationTranslatedDF.drop("Unit").drop("Sex").filter(educationTranslatedDF("Citizen") === "Total").drop("Citizen").withColumnRenamed("ISCED11", "Education_Attainment")
# MAGIC for((original, sub) <- euroMap) {
# MAGIC   educationOpaqueDF = educationOpaqueDF.withColumn("Country", regexp_replace(educationOpaqueDF("Country"), "\\Q" + original + "\\E", sub))
# MAGIC }
# MAGIC for((original, sub) <- educationAttainmentMap) {
# MAGIC   educationOpaqueDF = educationOpaqueDF.withColumn("Education_Attainment", regexp_replace(educationOpaqueDF("Education_Attainment"), "\\Q" + original + "\\E", sub))
# MAGIC }
# MAGIC for((original, sub) <- agesMap) {
# MAGIC   educationOpaqueDF = educationOpaqueDF.withColumn("Age", regexp_replace(educationOpaqueDF("Age"), original, sub))
# MAGIC }
# MAGIC val educationShortDF = educationOpaqueDF
# MAGIC 
# MAGIC display(educationShortDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Separate measurements by year.

# COMMAND ----------

# MAGIC %scala
# MAGIC var educationMapDF = educationShortDF.withColumn("year_map", map(educationYears.map(year => lit(year.toString) :: educationShortDF(year.toString) :: Nil).flatten: _*))
# MAGIC 
# MAGIC for (i <- educationYears) {
# MAGIC   educationMapDF = educationMapDF.drop(i.toString)
# MAGIC }
# MAGIC 
# MAGIC educationMapDF = educationMapDF.select(educationMapDF("*"), explode(educationMapDF("year_map")))
# MAGIC educationMapDF = educationMapDF.withColumnRenamed("key", "Year").withColumnRenamed("value", "Proportion").drop("year_map")
# MAGIC educationMapDF = educationMapDF.withColumn("Year", educationMapDF("Year").cast(IntegerType))
# MAGIC educationMapDF.cache()
# MAGIC 
# MAGIC display(educationMapDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Move education attainment proportions to columns and separate into younger, middle-aged and older proportions.

# COMMAND ----------

# MAGIC %scala
# MAGIC val educationGlobalDF = educationMapDF.filter(educationMapDF("Age") === "18-74")
# MAGIC .groupBy("Country", "Year").pivot("Education_Attainment").sum("Proportion")
# MAGIC 
# MAGIC val educationYoungDF = educationMapDF.filter(educationMapDF("Age") === "18-24" || educationMapDF("Age") === "25-34")
# MAGIC .groupBy("Country", "Year").pivot("Education_Attainment").sum("Proportion")
# MAGIC 
# MAGIC val educationMiddleDF = educationMapDF.filter(educationMapDF("Age") === "25-54")
# MAGIC .groupBy("Country", "Year").pivot("Education_Attainment").sum("Proportion")
# MAGIC 
# MAGIC val educationOldDF = educationMapDF.filter(educationMapDF("Age") === "55-74")
# MAGIC .groupBy("Country", "Year").pivot("Education_Attainment").sum("Proportion")
# MAGIC 
# MAGIC display(educationGlobalDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Join datasets.

# COMMAND ----------

# MAGIC %scala
# MAGIC val joinedCols = List("Country", "Year")
# MAGIC val globalJoinedDF = natalityMapDF.join(educationGlobalDF, joinedCols)
# MAGIC val youngJoinedDF = natalityMapDF.join(educationYoungDF, joinedCols)
# MAGIC val middleJoinedDF = natalityMapDF.join(educationMiddleDF, joinedCols)
# MAGIC val oldJoinedDF = natalityMapDF.join(educationOldDF, joinedCols)
# MAGIC display (globalJoinedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Save data to files.

# COMMAND ----------

# MAGIC %scala
# MAGIC val data_path = "dbfs:/FileStore/temp/analysis/"
# MAGIC globalJoinedDF.write.option("header", true).mode("overwrite").csv(data_path + "joined-global")
# MAGIC youngJoinedDF.write.option("header", true).mode("overwrite").csv(data_path + "joined-young")
# MAGIC middleJoinedDF.write.option("header", true).mode("overwrite").csv(data_path + "joined-middle-age")
# MAGIC oldJoinedDF.write.option("header", true).mode("overwrite").csv(data_path + "joined-old")
# MAGIC natalityMapDF.write.option("header", true).mode("overwrite").csv(data_path + "natality")
# MAGIC educationGlobalDF.write.option("header", true).mode("overwrite").csv(data_path + "education-global")
# MAGIC educationYoungDF.write.option("header", true).mode("overwrite").csv(data_path + "education-young")
# MAGIC educationMiddleDF.write.option("header", true).mode("overwrite").csv(data_path + "education-middle-age")
# MAGIC educationOldDF.write.option("header", true).mode("overwrite").csv(data_path + "education-old")

# COMMAND ----------

# MAGIC %md
# MAGIC Repartition education dataset for streaming.

# COMMAND ----------

# MAGIC %scala
# MAGIC val stream_path = "dbfs:/FileStore/temp/stream"
# MAGIC val educationStringDF = educationMapDF.withColumn("Year", educationMapDF("Year").cast(StringType))
# MAGIC val educationYearDF = educationStringDF.withColumn("Year", to_timestamp(educationStringDF("Year").cast(TimestampType)))
# MAGIC 
# MAGIC educationYearDF.write.partitionBy("Year").option("header", true).mode("overwrite").csv(stream_path)
# MAGIC 
# MAGIC display(educationYearDF)