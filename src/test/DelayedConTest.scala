import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}


object DelayedConTest {
    implicit val ec: ExecutionContextExecutor = ExecutionContext.global
    def main(args: Array[String]): Unit = {
        val connAttempt = Future {
            while (!Thread.interrupted()) {
                println("On connect")
                Thread.sleep(1000L)
            }
            true
        }

        //connAttempt
    }
}
