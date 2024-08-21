// Databricks notebook source
// MAGIC %md
// MAGIC ### Imports

// COMMAND ----------

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, StringType} 

// COMMAND ----------

sc.master :: 
sc.appName :: 
sc.uiWebUrl.get ::
sc.version ::
Nil mkString "\n"

// COMMAND ----------

// MAGIC %md
// MAGIC ### Implicit call Exchange SinglePartition operator

// COMMAND ----------

// MAGIC %md
// MAGIC Exchange SinglePartition collects data from the entire DataFrame into one batch per worker

// COMMAND ----------

val df = spark.range(0, 1000, 1, 10).localCheckpoint
df.show
df.rdd.getNumPartitions

// COMMAND ----------

val wholeDataset = Window.partitionBy().orderBy('id.asc)

// COMMAND ----------

val projectionBad = df.select('id, row_number().over(wholeDataset))

projectionBad.explain
projectionBad.sample(0.02).show(10)
projectionBad.rdd.getNumPartitions

// COMMAND ----------

val projectionGood = df.select('id, monotonically_increasing_id())

projectionGood.explain
projectionGood.sample(0.02).show(10)
projectionGood.rdd.getNumPartitions

// COMMAND ----------

// MAGIC %md
// MAGIC Pre-sorting in each partition will increase the speed and performance of data processing for further purposes, especially the exclusion of the Exchange SinglePartition operator and the preservation of parallelism. It is also necessary to take into account that sorting occurs based on some assumptions that the data does not intersect with each other.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Partial Caching

// COMMAND ----------

spark.sharedState.cacheManager.clearCache

// COMMAND ----------

val dfBad = spark.range(0, 10000, 1, 100)
dfBad.cache()
dfBad.show(2000)

// COMMAND ----------

sc.getRDDStorageInfo.foreach { crdd =>
    println(s"${crdd.name.trim}: ${crdd.numCachedPartitions}/${crdd.numPartitions}")
    println("####")
}

// COMMAND ----------

// MAGIC %md
// MAGIC Currently, 25 of 100 partitions are cached, so during further processing the data may not be consistent due to external interaction. To avoid this possibility, it is necessary to use the cache - count relationship.

// COMMAND ----------

val dfGood = spark.range(0, 10000, 1, 100)
dfGood.cache()
dfGood.count

// COMMAND ----------

sc.getRDDStorageInfo.foreach { crdd =>
    println(s"${crdd.name.trim}: ${crdd.numCachedPartitions}/${crdd.numPartitions}")
    println("####")
}

// COMMAND ----------

// MAGIC %md
// MAGIC As you can see, after using this method, 100 out of 100 partitions are already cached.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Reduced parallelism when using coalesce

// COMMAND ----------

val udf_sleep = udf { () => { Thread.sleep(1000); null} }

// COMMAND ----------

val df = spark.range(0, 10, 1, 2)
spark.time { 
    df.select(udf_sleep()).collect
    df.rdd.getNumPartitions
}

// COMMAND ----------

val df = spark.range(0, 10, 1, 2)
spark.time { 
    val data = df.select(udf_sleep()).coalesce(1)
    data.explain
    data.collect
    data.rdd.getNumPartitions
}

// COMMAND ----------

// MAGIC %md
// MAGIC Execution time has doubled

// COMMAND ----------

val df = spark.range(0, 10, 1, 2)
spark.time { 
    val data = df.select(udf_sleep()).localCheckpoint.coalesce(1)
    data.explain
    data.collect
    data.rdd.getNumPartitions
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Type mismatch when working with JSON

// COMMAND ----------

// MAGIC %md
// MAGIC In case of small type mismatches between the schema and the JSON document, the parsing function returns null

// COMMAND ----------

val testJsonString = """{ "foo": "bar", "number": 3000000000 }"""

val schema = StructType(StructField("foo", StringType) :: StructField("number", IntegerType) :: Nil)

val df = spark.range(1).select(from_json(lit(testJsonString), schema).alias("f")).select($"f.*").show(1)

// COMMAND ----------

val testJsonString = """{ "foo": "bar", "number": 2000000000 }"""

val schema = StructType(StructField("foo", StringType) :: StructField("number", IntegerType) :: Nil)

val df = spark.range(1).select(from_json(lit(testJsonString), schema).alias("f")).select($"f.*").show(1)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Using Dataset API Transformations

// COMMAND ----------

// MAGIC %md
// MAGIC Under the hood, the Dataset API may have additional physical operators

// COMMAND ----------

val df = spark.range(1000).localCheckpoint

df.select('id + 1).explain

// COMMAND ----------

val df = spark.range(1000).as[Long].localCheckpoint

df.map { x => x + 1 }.explain

// COMMAND ----------

val df = spark.range(1000).as[Long].localCheckpoint

df.filter { x => x > 0 }.explain

// COMMAND ----------

// MAGIC %md
// MAGIC ### Passing null to Scala UDF

// COMMAND ----------

// MAGIC %md
// MAGIC If null is passed to one of the arguments of a Scala UDF, the function may return null without actually being called

// COMMAND ----------

def simpleFunc[T](x: T) = 1

// COMMAND ----------

val int_udf = udf { simpleFunc[Int] _ } // (x: Int) => 1
val long_udf = udf { simpleFunc[Long] _ } // (x: Long) => 1
val string_udf = udf { simpleFunc[String] _ } // (x: String) => 1

// COMMAND ----------

val df = spark.range(1)
df.select(int_udf(lit(1)), long_udf(lit(1L)), string_udf(lit("1"))).show

// COMMAND ----------

val df = spark.range(1)
df.select(
    int_udf(lit(null)), 
    long_udf(lit(null)), 
    string_udf(lit(null))).show

// COMMAND ----------

// MAGIC %md
// MAGIC - String == java.lang.String
// MAGIC - Int != java.lang.Integer
// MAGIC - Long != java.lang.Long

// COMMAND ----------

val java_int_udf = udf { simpleFunc[java.lang.Integer] _ }
val java_long_udf = udf { simpleFunc[java.lang.Long] _ }
val java_string_udf = udf { simpleFunc[java.lang.String] _ }

// COMMAND ----------

val df = spark.range(1)
df.select(java_int_udf(lit(null)), java_long_udf(lit(null)), java_string_udf(lit(null))).show

// COMMAND ----------

// MAGIC %md
// MAGIC ### Large number of partitions when writing

// COMMAND ----------

// MAGIC %md
// MAGIC The amount of RAM required for Hadoop NameNode to operate depends on the number of files in the proportion 1 million files = 1 GB of RAM

// COMMAND ----------

val df = spark.range(0, 1000000, 1, 100)
df
    .withColumn("pmod", pmod('id, lit(20)))
    .write
    .partitionBy("pmod")
    .mode("overwrite")
    .parquet("/tmp/datasets/test.parquet.bad")

// COMMAND ----------

// MAGIC %sh find /dbfs/tmp/datasets/test.parquet.bad/ -type f | wc -l

// COMMAND ----------

// MAGIC %sh ls /dbfs/tmp/datasets/test.parquet.bad/pmod=1

// COMMAND ----------

val df = spark.range(0, 1000000, 1, 100)
df
    .withColumn("pmod", pmod('id, lit(20)))
    .repartition(10, col("pmod"))
    .write
    .partitionBy("pmod")
    .mode("overwrite")
    .parquet("/tmp/datasets/test.parquet.good")

// COMMAND ----------

// MAGIC %sh find /dbfs/tmp/datasets/test.parquet.good/ -type f | wc -l

// COMMAND ----------

// MAGIC %sh ls /dbfs/tmp/datasets/test.parquet.good/pmod=1

// COMMAND ----------

val path = "/tmp/datasets"

dbutils.fs.rm(path, recurse = true)
