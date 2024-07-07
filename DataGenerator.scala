package scala
import java.io.PrintStream
import java.net.ServerSocket
import java.sql.Timestamp
import scala.util.Random
import java.text.SimpleDateFormat

object DataGenerator {

  val random = new Random()

  // Generating a random date within Åa given range
  def generateRandomTimestamp(start: Timestamp, end: Timestamp): Timestamp = {
    val diff = end.getTime - start.getTime + 1
    new Timestamp(start.getTime + (random.nextLong() % diff + diff) % diff)
  }

  // Generating a random event type
  def generateRandomEventType(): String = {
    val eventTypes = Array("AddToCart", "Purchase", "PageView", "RemoveFromCart")
    eventTypes(random.nextInt(eventTypes.length))
  }

  // Generating random data for an event
  def generateRandomEvent(start: Timestamp, end: Timestamp, maxPrice: Double): (Timestamp, String, String, String, Double) = {
    val timestamp = generateRandomTimestamp(start, end)
    val customerId = "CId" + (random.nextInt(100) + 1)
    val eventType = generateRandomEventType()
    val productId = "PId" + (random.nextInt(10000) + 1)
    val price = random.nextDouble() * maxPrice
    (timestamp, customerId, eventType, productId, price)
  }

  def main(args: Array[String]): Unit = {

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val startDate = dateFormat.parse("2024-01-01 00:00:00")
    val endDate = dateFormat.parse("2024-01-31 23:59:59")
    val startTimestamp = new Timestamp(startDate.getTime)
    val endTimestamp = new Timestamp(endDate.getTime)
    val maxPrice = 100.0

    // Generating and sending data via a socket
    val socket = new ServerSocket(9999).accept()
    val printStream = new PrintStream(socket.getOutputStream, true)
    while (true) {
      val event = generateRandomEvent(startTimestamp, endTimestamp, maxPrice)
      printStream.println(event)
      Thread.sleep(5000)
    }
  }
}
