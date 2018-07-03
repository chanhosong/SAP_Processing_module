package com.hhi.sap.server

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives.{complete, get, path}
import akka.stream.ActorMaterializer
import com.hhi.sap.main.SparkSessionTestWrapper
import org.scalatest.FunSuite

import scala.io.StdIn

class WebServerTest extends FunSuite with SparkSessionTestWrapper{

  test("WebServer Test") {
    val IP = "10.25.12.59"
    val PORT = 8080

    val OUTPUTPATH = "src/test/resources/output/"
    val TABLE2 = "table2"

    implicit val system = ActorSystem("my-system")
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

    val df = ss.read.option("header", "true").csv(OUTPUTPATH+TABLE2).where("ranking == 1")
    df.show()

    val route =
      path("rank") {
        get {
          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, df.collectAsList().toString))
        }
      }

    val bindingFuture = Http().bindAndHandle(route, IP, PORT)

    println(s"Server online at http://$IP:$PORT/rank\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }
}
