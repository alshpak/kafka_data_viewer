package devtools.kafka_data_viewer.kafkaconn

import java.time.Instant

import devtools.kafka_data_viewer.kafkaconn.Connector._
import devtools.lib.rxext.Observable.{empty, just}
import devtools.lib.rxext.{Observable, Subject}

import scala.util.Random

object StubConnector {

    def stubConnector(): Connector = new Connector {

        override def connectConsumer(): ConsumerConnection = new ConsumerConnection {

            override def queryTopics(): Seq[String] = Seq("topic1", "topic2", "topic3")

            override def queryTopicsWithSizes(): Seq[(String, Long)] = queryTopics().zipWithIndex.map(x => x._1 -> x._2.toLong)

            override def readNextRecords()(executionMonitor: Option[Subject[String]], stop: Option[Observable[Unit]]): Observable[Seq[BinaryTopicRecord]] = empty()

            override def readTopicsContinually(topics: Observable[Seq[String]]): Observable[Seq[BinaryTopicRecord]] = {
                val publisher = Subject.publishSubject[Seq[BinaryTopicRecord]]()

//                val currentTopics = Subject.behaviorSubject(Seq[String]())
//                (currentTopics <<< topics)($)
//                new Thread { () =>
//                    publisher << (for (topic <- currentTopics.value; amount <- 0 to Random.nextInt(5)) yield
//                        TopicRecord(partition = Random.nextInt(3), offset = 1, time = Instant.now(), topic = topic, key = "some key", value = "Some value", msgtype = StringMessage))
//                    Thread.sleep(1000)
//                }.start()

                publisher
            }

            override def readTopic(topic: String, limitPerPartition: Long)(executionMonitor: Option[Subject[String]], stop: Option[Observable[Unit]]): Observable[Seq[BinaryTopicRecord]] = {
                just(/*for {
                    idx <- 1.to(Random.nextInt(100))
                    rec = TopicRecord(partition = Random.nextInt(3), offset = 1, time = Instant.now(), topic = topic, key = "some key", value = "Some value", msgtype = StringMessage)
                } yield rec*/)
            }

            override def close(): Unit = Unit

            override def queryTopicPartitions(topic: String): Seq[Int] = ???
        }

        override def connectProducer(): ProducerConnection = new ProducerConnection {

            override def close(): Unit = Unit

            override def send(topic: String, key: Array[Byte], value: Array[Byte], partition: PartitionerMode): Unit = ???
        }

        override def closeAll(): Unit = Unit
    }

}
