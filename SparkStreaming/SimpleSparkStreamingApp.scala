package scala
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.streaming.OutputMode

object TaxiAnalyzer extends App {

  val spark = SparkSession.builder()
    .appName("ReviewAnalyzer")
    .master("local[*]")
    .getOrCreate()

  spark.conf.set("spark.sql.session.timeZone", "UTC")

  val startTime = System.currentTimeMillis()

  import spark.implicits._

  case class Event(timestamp: String, customerId: String, eventType: String, productId: String, price: Double, processingDuration: Double)

  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
    .as[String]

  val events = lines.map { line =>
    val Array(timestamp, clientId, action, productId, value) = line.stripPrefix("(").stripSuffix(")").split(",")
    val price = value.toDouble
    val endTime = System.currentTimeMillis()
    val processingDuration = endTime - startTime
    Event(timestamp, clientId, action, productId, price, processingDuration)
  }

  // Outputting data to the console
  val query = events.writeStream
    .outputMode(OutputMode.Update())
    .format("console")
    .option("truncate", false)
    .start()

  query.awaitTermination()

}