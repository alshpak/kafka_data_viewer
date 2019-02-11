import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringSerializer}

import scala.collection.JavaConverters._

object KafkaDataPolluter {

    def main(args: Array[String]): Unit = {

        val host = "kafka-docker:9092"
        val consumerProps = Map(
            "bootstrap.servers" -> host
            , "group.id" -> "local.data"
            , "key.deserializer" -> classOf[ByteArrayDeserializer].getName
            , "value.deserializer" -> classOf[ByteArrayDeserializer].getName
            , "enable.auto.commit" -> "false"
            , ConsumerConfig.FETCH_MAX_BYTES_CONFIG -> new Integer(100000)
            , ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG -> new Integer(100000))

        val producerProps = Map(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> host,
            ProducerConfig.ACKS_CONFIG -> "all",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getName,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName)

        val producer = new KafkaProducer[String, Array[Byte]]((producerProps: Map[String, Object]).asJava)

        /*
        producer.send(
           // new ProducerRecord("nulltopic", "test", "test".getBytes)
            new ProducerRecord("nulltopic", null, null)
        ).get()
        System.exit(0)
        */

        for (topic <- 0.to(5)) {
            for (msg <- 0.to(20000)) {
                producer.send(new ProducerRecord[String, Array[Byte]]("jsontopic" + topic, "key" + msg, someJson(msg).getBytes("UTF8")))
            }
        }

    }

    def someJson(key: Int): String =
        s"""
           |{
           |     "data": {
           |         "key": $key,
           |         "values": [
           |             { "object" : "obj$key" },
           |             { "object" : "other object" }
           |         ]
           |     }
           |}
        """.stripMargin


}
