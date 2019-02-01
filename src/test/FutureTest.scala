import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object FutureTest {

    def main(args: Array[String]): Unit = {

        val f = Future {
            try {
                Thread.sleep(100000L)
                true
            } catch {
                case e: Exception => println("Exception " + e)
            }
        }
        try {
            val r = Await.result(f, 2 seconds)
            println("Completed with " + r)
        } catch {
            case e: Exception => println("Await is not done: " + e)
        }

        Thread.sleep(5000L)
    }
}
