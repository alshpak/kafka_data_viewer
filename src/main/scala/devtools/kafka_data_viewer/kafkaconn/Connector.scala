package devtools.kafka_data_viewer.kafkaconn

import java.time.Instant

import devtools.kafka_data_viewer.kafkaconn.Connector.{ConsumerConnection, ProducerConnection}
import devtools.lib.rxext.{Observable, Subject}

object Connector {

    case class BinaryTopicRecord(topic: String, partition: Int, offset: Long, time: Instant, key: Array[Byte], value: Array[Byte])

    sealed trait PartitionerMode

    case object DefaultPartitioner extends PartitionerMode

    case class ExactPartition(partition: Int) extends PartitionerMode

    def `with`[T, R](value: => T)(f: T => R): R = f(value)

    def withClosable[T <: AutoCloseable, R](f: () => T)(p: T => R): R = `with`(f())(res => try p(res) finally res.close())

    type TopicsWithSizes = Seq[(String, Long)]

    trait ConsumerConnection {

        def queryTopicPartitions(topic: String): Seq[Int]

        def queryTopics(): Seq[String]

        def queryTopicsWithSizes(): TopicsWithSizes

        def readNextRecords()(executionMonitor: Option[Subject[String]] = None, stop: Option[Observable[Unit]] = None): Observable[Seq[BinaryTopicRecord]]

        def readTopicsContinually(topics: Observable[Seq[String]]): Observable[Seq[BinaryTopicRecord]]

        def readTopic(topic: String, limitPerPartition: Long)(executionMonitor: Option[Subject[String]] = None, stop: Option[Observable[Unit]] = None): Observable[Seq[BinaryTopicRecord]]

        def close(): Unit
    }

    trait ProducerConnection {

        def send(topic: String, key: Array[Byte], value: Array[Byte], partition: PartitionerMode): Unit

        def close(): Unit

    }

}

trait Connector {

    def connectConsumer(): ConsumerConnection

    def connectProducer(): ProducerConnection

    def closeAll(): Unit

}

