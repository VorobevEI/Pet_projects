package controllers

import javax.inject._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import play.api.mvc._
import play.filters.csrf.CSRF
import scala.concurrent.{ExecutionContext, Future}
import java.util.Properties
import controllers.routes

@Singleton
class KafkaController @Inject()(cc: ControllerComponents)(implicit exec: ExecutionContext) extends AbstractController(cc) {

  // Kafka Configuration
  private val kafkaProps = new Properties()
  kafkaProps.put("bootstrap.servers", "localhost:9092")
  kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  private val kafkaProducer = new KafkaProducer[String, String](kafkaProps)

  // Rendering form with CSRF token
  def showForm: Action[AnyContent] = Action { implicit request =>
    val csrfToken = CSRF.getToken.map(_.value).getOrElse("")
    val htmlForm =
      s"""
         |<html>
         |<head>
         |  <title>Kafka Message Sender</title>
         |  <style>
         |    body {
         |      font-family: Arial, sans-serif;
         |      background-color: #f4f4f4;
         |      text-align: center;
         |      padding: 50px;
         |    }
         |    form {
         |      display: inline-block;
         |      background-color: #fff;
         |      padding: 20px;
         |      border-radius: 8px;
         |      box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
         |    }
         |    input[type="text"], input[type="hidden"] {
         |      width: 80%;
         |      padding: 10px;
         |      margin: 10px 0;
         |      border: 1px solid #ccc;
         |      border-radius: 4px;
         |    }
         |    button {
         |      padding: 10px 20px;
         |      background-color: #007bff;
         |      color: white;
         |      border: none;
         |      border-radius: 4px;
         |      cursor: pointer;
         |      transition: background-color 0.3s ease;
         |    }
         |    button:hover {
         |      background-color: #0056b3;
         |    }
         |  </style>
         |</head>
         |<body>
         |  <h1>Send a Message to Kafka</h1>
         |  <form action="/send-message" method="post">
         |    <input type="hidden" name="csrfToken" value="$csrfToken" />
         |    <input type="text" name="message" placeholder="Enter your message" required />
         |    <button type="submit">Send</button>
         |  </form>
         |</body>
         |</html>
         |""".stripMargin
    Ok(htmlForm).as(HTML)
  }

  // Handling sent message
  def sendMessage: Action[AnyContent] = Action.async { implicit request =>
    request.body.asFormUrlEncoded match {
      case Some(formData) =>
        val message = formData.get("message").flatMap(_.headOption).getOrElse("")
        val csrfToken = formData.get("csrfToken").flatMap(_.headOption).getOrElse("")
        if (message.nonEmpty && csrfToken.nonEmpty) {
          // Sending message to Kafka
          kafkaProducer.send(new ProducerRecord[String, String]("my_topic", message))
          // Redirect back to chat form
          Future.successful(Redirect(routes.KafkaController.showForm))
        } else {
          Future.successful(BadRequest("Message cannot be empty or CSRF token is missing"))
        }
      case None =>
        Future.successful(BadRequest("Invalid form submission"))
    }
  }
}
