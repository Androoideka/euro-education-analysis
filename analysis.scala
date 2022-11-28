// Databricks notebook source
// MAGIC %md
// MAGIC Load datasets.

// COMMAND ----------

// MAGIC %fs ls "dbfs:/FileStore/temp/analysis/"

// COMMAND ----------

val natalityFile = "dbfs:/FileStore/temp/analysis/natality/"
val natalityDF = spark.read.format("csv").option("header", true).option("inferSchema", true).load(natalityFile)
natalityDF.cache()

val natalityNoEuro = natalityDF.filter(!natalityDF("Country").contains("EU"))
val natalityOnlyEuro = natalityDF.filter(natalityDF("Country").contains("EU") && natalityDF("Year") =!= 2020)
natalityOnlyEuro.cache()

display(natalityDF)

// COMMAND ----------

val educationGlobalFile = "dbfs:/FileStore/temp/analysis/education-global/"
val educationGlobalDF = spark.read.format("csv").option("header", true).option("inferSchema", true).load(educationGlobalFile)
educationGlobalDF.cache()

val educationNoEuro = educationGlobalDF.filter(!educationGlobalDF("Country").contains("EU"))
val educationOnlyEuro = educationGlobalDF.filter(educationGlobalDF("Country").contains("EU") && educationGlobalDF("Year") =!= 2004)
educationOnlyEuro.cache()

display(educationGlobalDF)

// COMMAND ----------

val joinedGlobalFile = "dbfs:/FileStore/temp/analysis/joined-global/"
val joinedGlobalDF = spark.read.format("csv").option("header", true).option("inferSchema", true).load(joinedGlobalFile)
joinedGlobalDF.cache()

val joinedOnlyEuroDF = joinedGlobalDF.filter(joinedGlobalDF("Country").contains("EU") && joinedGlobalDF("Year") > 2008)
joinedOnlyEuroDF.cache()

// COMMAND ----------

// MAGIC %md
// MAGIC Chart of natality across the European Union over the years. Notice how births have steadily gone down. 2020 was excluded because not all data from that year is present.

// COMMAND ----------

val natalityChart = natalityOnlyEuro.filter(natalityOnlyEuro("Country") === "EU28")
display(natalityChart)

// COMMAND ----------

// MAGIC %md
// MAGIC Chart of education across the European Union over the years. 2004 was excluded because not all data from that year is present.

// COMMAND ----------

val educationWiderEUChart = educationOnlyEuro.filter(educationOnlyEuro("Country") === "EU28")
display(educationWiderEUChart)

// COMMAND ----------

// MAGIC %md
// MAGIC Chart of education for the main European Union members. 2004 was excluded because not all data from that year is present.

// COMMAND ----------

val educationCoreEUChart = educationOnlyEuro.filter(educationOnlyEuro("Country") === "EU15")
display(educationCoreEUChart)

// COMMAND ----------

// MAGIC %md
// MAGIC Hypotheses

// COMMAND ----------

// MAGIC %md
// MAGIC Hypothesis 1: Birth rate in the most developed European Union countries has declined as a consequence of higher education levels of European women.

// COMMAND ----------

val joinedDevelopedEuroDF = joinedOnlyEuroDF.filter(joinedOnlyEuroDF("Country") === "EU19")
display(joinedDevelopedEuroDF)

// COMMAND ----------

// MAGIC %md
// MAGIC As we can see from the graph, birth rates in core member EU states have declined, but the proportion of European women enrolling in tertiary education has also declined. Thus, birth rate has not gone down due to increased education levels, because the education level of women has not increased.

// COMMAND ----------

// MAGIC %md
// MAGIC Hypothesis 2: Proportion of highly educated women in Europe has declined because of lower levels of education in the newer member states of the European Union.

// COMMAND ----------

val educationTestEuro = educationOnlyEuro.filter(educationOnlyEuro("Country") =!= "EU27" && !educationOnlyEuro("Country").contains("EU19"))
display(educationTestEuro)

// COMMAND ----------

// MAGIC %md
// MAGIC Comparing higher education proportions across core EU member states and all EU member states, we can see that the enlargement of the EU did not contribute to the decrease of the proportion of tertiary-educated women, and even increased it instead.

// COMMAND ----------

// MAGIC %md
// MAGIC Hypothesis 3: Birth rates of newer member states have skewed the European Union's birth rate trend.

// COMMAND ----------

val natalityTestEuro = natalityOnlyEuro.filter(natalityOnlyEuro("Country") === "EU18" || natalityOnlyEuro("Country").contains("EU28"))
display(natalityTestEuro)

// COMMAND ----------

// MAGIC %md
// MAGIC The graph shows that the birth rate trend is similar even after factoring in newer EU member states.

// COMMAND ----------

// MAGIC %md
// MAGIC Hypothesis 4: There are no European Union member states whose birth rate has gone up in the past few years.

// COMMAND ----------

val natalityRaiseDF = natalityNoEuro.filter(natalityNoEuro("Year") > 2017)
display(natalityRaiseDF)

// COMMAND ----------

val natalityIcelandDF = natalityNoEuro.filter(natalityNoEuro("Country") === "Iceland")
display(natalityIcelandDF)

// COMMAND ----------

// MAGIC %md
// MAGIC There are no EU member states whose birth rate has gone up in 2020 compared to their levels before 2018.