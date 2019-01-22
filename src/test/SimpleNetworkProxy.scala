import java.io.{InputStream, OutputStream}
import java.net.{ServerSocket, Socket}

import scala.language.postfixOps

object SimpleNetworkProxy {

    def main(args: Array[String]): Unit = {

        def monitor(prefix: String)(content: Array[Byte], len: Int): Unit = {
            println(prefix + " transferred " + len + "bytes")
        }

        val serverSocket = new ServerSocket()
        while (true) {
            val socket = serverSocket.accept()
            inThread {
                val targetSocket = new Socket("kafka-docker", 9092)
                val t1 = inThread(redirectStream(socket.getInputStream, targetSocket.getOutputStream)(monitor("S->T")))
                val t2 = inThread(redirectStream(targetSocket.getInputStream, socket.getOutputStream)(monitor("T->S")))
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
        Stream.continually(in.read(buf)).takeWhile(-1 !=)
                .foreach(len => {
                    interAction(buf, len)
                    out.write(buf, 0, len)
                })
    }
}
