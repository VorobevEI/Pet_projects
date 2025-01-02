import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, current_timestamp, expr, window}

import java.sql.DriverManager

object KafkaToPGApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaToPGApp")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val profanityMap = Map(
      "en" -> List(
        "damn", "hell", "crap", "bastard", "bloody",
        "asshole", "dick", "shit", "fuck", "bitch"
      ),
      "de" -> List(
        "verdammt", "scheisse", "arschloch", "wichser",
        "fotze", "drecksau", "hure", "miststück", "schlampe"
      ),
      "fr" -> List(
        "merde", "putain", "con", "enculé", "salope",
        "bordel", "connard", "foutre", "bite", "chier"
      ),
      "es" -> List(
        "mierda", "puta", "cabron", "coño", "joder",
        "gilipollas", "zorra", "hostia", "cabrón", "maldito"
      )
    )

    val broadcastProfanity = spark.sparkContext.broadcast(profanityMap)

    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "my_topic")
      .option("includeTimestamp", "true")
      .load()

    val valueStream = kafkaStream.selectExpr("CAST(value AS STRING)")
      .withColumn("timestamp", current_timestamp())

    val processedStream = valueStream.withColumn("contains_profanity", expr {
      val conditions = broadcastProfanity.value.flatMap {
        case (lang, words) =>
          words.map(word => s"value LIKE '%$word%'")
      }.mkString(" OR ")
      s"CASE WHEN $conditions THEN true ELSE false END"
    })
      .withColumn("language", expr {
        val cases = broadcastProfanity.value.map {
          case (lang, words) =>
            val conditions = words.map(word => s"value LIKE '%$word%'").mkString(" OR ")
            s"WHEN $conditions THEN '$lang'"
        }.mkString(" ")
        s"CASE $cases ELSE 'clean language' END"
      })

    val query = processedStream.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], batchId: Long) =>

        val connection = DriverManager.getConnection(
          "jdbc:postgresql://localhost:5432/your_database",
          "your_user",
          "your_password"
        )

        connection.setAutoCommit(false)

        batchDF.write
          .format("jdbc")
          .option("url", "jdbc:postgresql://localhost:5432/your_database")
          .option("dbtable", "word_counts")
          .option("user", "your_user")
          .option("password", "your_password")
          .mode("append")
          .save()

        connection.commit()
        connection.close()

      }
      .start()

    query.awaitTermination()
  }
}
