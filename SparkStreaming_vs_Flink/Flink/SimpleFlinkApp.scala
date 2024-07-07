import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

package scala {
  case class Event(timestamp: String, customerId: String, eventType: String, productId: String, price: Double, processingDuration: Double)


  object FlinkStreamingExample {

    def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment

      val startTime = System.currentTimeMillis()

      // Read data from the socket
      val dataStream = env.socketTextStream("localhost", 9999, '\n')

      // Convert strings to Event objects and add processing duration
      val eventStream = dataStream.map { line =>
        val Array(timestamp, customerId, eventType, productId, priceStr) = line.stripPrefix("(").stripSuffix(")").split(",")
        val price = priceStr.toDouble
        val endTime = System.currentTimeMillis()
        val processingDuration = endTime - startTime
        Event(timestamp, customerId, eventType, productId, price, processingDuration)
      }

      // Outputting data to the console
      eventStream.print()

      // Starting a data stream
      env.execute("Flink Socket Streaming Example")
    }
  }
}
