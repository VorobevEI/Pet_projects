package controllers

import javax.inject._
import org.apache.pekko.actor.ActorSystem
import play.api.libs.json._
import play.api.mvc._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

@Singleton
class AsyncController @Inject()(cc: ControllerComponents, actorSystem: ActorSystem)(implicit exec: ExecutionContext) extends AbstractController(cc) {

  // Обработка параметра из URL
  def echoMessage(msg: String) = Action.async {
    Future {
      Ok(s"You sent: $msg")
    }
  }

  // Обработка тела POST-запроса
  def postMessage = Action.async(parse.json) { request =>
    (request.body \ "message").asOpt[String] match {
      case Some(message) =>
        Future {
          Ok(Json.obj("status" -> "success", "echo" -> message))
        }
      case None =>
        Future {
          BadRequest(Json.obj("status" -> "error", "message" -> "Missing 'message' field"))
        }
    }
  }

  // Пример с отложенным сообщением (оставляем из оригинального кода)
  def message = Action.async {
    getFutureMessage(1.second).map { msg => Ok(msg) }
  }

  private def getFutureMessage(delayTime: FiniteDuration): Future[String] = {
    val promise: Promise[String] = Promise[String]()
    actorSystem.scheduler.scheduleOnce(delayTime) {
      promise.success("Hello from Play Framework!")
    }(actorSystem.dispatcher)
    promise.future
  }
}
