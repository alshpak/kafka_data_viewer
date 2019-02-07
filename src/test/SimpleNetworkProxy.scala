import java.io.{InputStream, OutputStream}
import java.net.{ServerSocket, Socket}
import java.util.concurrent.atomic.AtomicInteger

import scala.language.postfixOps

object SimpleNetworkProxy {

    def main(args: Array[String]): Unit = {

        def monitor(prefix: String)(content: Array[Byte], len: Int): Unit = {
            val printableContent = content
                    .foldLeft((List[Byte](), false))((acc, el) =>
                        if (el >= 32 && el <= 127) (el :: acc._1, false)
                        else if (!acc._2) ('?'.toByte :: acc._1, true)
                        else acc)
                    ._1.reverse

            println(prefix + " transferred " + len + "bytes => " + new String(printableContent.toArray))
        }

        val serverSocket = new ServerSocket(19092)
        val counter = new AtomicInteger()
        while (true) {
            val socket = serverSocket.accept()
            inThread {
                val idx = counter.incrementAndGet()
                monitor(s"[$idx]Connected")(Array[Byte](), 0)
                val targetSocket = new Socket("kafka-docker", 9092)
                val t1 = inThread(redirectStream(socket.getInputStream, targetSocket.getOutputStream)(monitor(s"[$idx]S->T")))
                val t2 = inThread(redirectStream(targetSocket.getInputStream, socket.getOutputStream)(monitor(s"[$idx]T->S")))
                t1.join()
                t2.join()
                targetSocket.close()
                socket.close()
            }
        }
    }

    def inThread(op: => Unit): Thread = {val t = new Thread(() => op); t.start(); t }

    def redirectStream(in: InputStream, out: OutputStream)(interAction: (Array[Byte], Int) => Unit): Unit = {
        val buf = Array.ofDim[Byte](4096)
        try {
            Stream.continually(in.read(buf)).takeWhile(-1 !=)
                    .foreach(len => {
                        interAction(buf, len)
                        out.write(buf, 0, len)
                    })
        } catch {
            case e: Exception => println("Pipe is closed due to " + e.getMessage)
        }
    }
}
