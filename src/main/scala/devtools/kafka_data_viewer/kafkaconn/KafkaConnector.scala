package devtools.kafka_data_viewer.kafkaconn

import java.time.Duration.ofSeconds
import java.time.{Duration, Instant}
import java.util
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger

import devtools.kafka_data_viewer.kafkaconn.Connector._
import devtools.kafka_data_viewer.kafkaconn.CustomPartitioner.DelegatingPartitioner
import devtools.lib.rxext.{Observable, Subject}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, Partitioner, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringSerializer}
import org.apache.kafka.common.{Cluster, PartitionInfo, TopicPartition}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps

class KafkaConnector(host: String,
                     groupName: String) extends Connector {


    private val consumers = ArrayBuffer[ConsumerConnection]()
    private val producers = ArrayBuffer[ProducerConnection]()

    override def connectConsumer(): ConsumerConnection = {
        val connection = new KafkaConsumerConnection(host, groupName)
        consumers += connection
        connection
    }

    override def connectProducer(): ProducerConnection = {
        val connection = new KafkaProducerConnection(host, groupName)
        producers += connection
        connection
    }

    override def closeAll(): Unit = {
        consumers foreach (_.close())
        producers foreach (_.close())
    }

}

object Counter {
    val counter = new AtomicInteger(0)
}

class KafkaConsumerConnection(host: String,
                              groupName: String) extends ConsumerConnection {

    private val consumerProps = Map(
        "bootstrap.servers" -> host
        , "group.id" -> groupName
        , "key.deserializer" -> classOf[ByteArrayDeserializer].getName
        , "value.deserializer" -> classOf[ByteArrayDeserializer].getName
        , "enable.auto.commit" -> "false"
        , ConsumerConfig.FETCH_MAX_BYTES_CONFIG -> new Integer(100000)
        , ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG -> new Integer(100000))


    type GenRecord = ConsumerRecord[Array[Byte], Array[Byte]]

    var closed = false
    val opSemaphore = new Semaphore(1, false)

    println("Connect with " + (consumerProps: Map[String, Object]).asJava.get("bootstrap.servers"))
    private val consumer = new KafkaConsumer[Array[Byte], Array[Byte]]((consumerProps: Map[String, Object]).asJava)

    override def queryTopics(): Seq[String] = operation {
        println("Query topics")
        consumer.listTopics().asScala.filterNot(_._1.startsWith("_")).keys.toSeq.sorted
    }

    override def queryTopicsWithSizes(): Seq[(String, Long)] = operation {
        println("Query topics with sizes")
        val allTopics: mutable.Map[String, util.List[PartitionInfo]] = consumer.listTopics().asScala
        val topics = allTopics.filterNot(_._1.startsWith("_"))
        val topicPartitions: Seq[TopicPartition] = topics.values.flatMap(x => x.asScala)
                .map(info => new TopicPartition(info.topic(), info.partition())).toSeq
        val beginnings = consumer.beginningOffsets(topicPartitions.asJava).asScala.toMap
        val endings = consumer.endOffsets(topicPartitions.asJava).asScala.toSeq
        val sizes = endings.map { case (ref, end) => ref -> (end - beginnings.getOrElse(ref, end)) }
        val result = sizes.foldLeft(Map[String, Long]())((acc, el) => acc + (el._1.topic() -> (el._2 + acc.getOrElse(el._1.topic(), 0L)))).toSeq
        result.sortBy(_._1)
    }

    private def buildBinaryRecord(r: ConsumerRecord[Array[Byte], Array[Byte]]): BinaryTopicRecord =
        BinaryTopicRecord(topic = r.topic(), partition = r.partition(), offset = r.offset(), time = Instant.ofEpochMilli(r.timestamp()), key = r.key(), value = r.value())

    override def readNextRecords()(executionMonitor: Option[Subject[String]], stop: Option[Observable[Unit]]): Observable[Seq[BinaryTopicRecord]] = {
        println("read next topics")
        var read = 0
        var stopped = false
        for (stopAction <- stop; _ <- stopAction) stopped = true
        Observable.create(obs => operation {
            for (recs <- Stream.continually(consumer.poll(ofSeconds(3))).takeWhile(!_.isEmpty && !stopped)) {
                val records = recs.asScala.toSeq
                read += records.size
                for (mon <- executionMonitor) mon onNext ("Read " + read)
                obs onNext records.map(buildBinaryRecord)
            }
            for (mon <- executionMonitor) mon onNext ("Completed read " + read)
            obs onComplete()
        })
    }

    override def readTopicsContinually(topics: Observable[Seq[String]]): Observable[Seq[BinaryTopicRecord]] = {
        println("read topics continually")

        def assignToTopics(topics: Seq[String]): Unit = {
            val topicPartitions = consumer.listTopics().asScala
                    .filter(x => topics.contains(x._1))
                    .toSeq
                    .flatMap(x => x._2.asScala.map(info => new TopicPartition(info.topic(), info.partition())))
                    .asJava

            consumer.assign(topicPartitions)
            consumer.seekToEnd(topicPartitions)
        }

        Observable.create(obs => operation {

            assignToTopics(Seq())
            var assignment = Seq[String]()
            var newTopics: Seq[String] = null
            topics.subscribe(topics => newTopics = topics)
            val getNewTopics = () => {val r = newTopics; newTopics = null; r }

            val reassign = (newTopics: Seq[String]) => if (newTopics != null) {
                assignToTopics(newTopics)
                assignment = newTopics
            }

            val nextRecords: Unit => Seq[GenRecord] = _ =>
                if (assignment.isEmpty) {Thread.sleep(1000); Seq() }
                else consumer.poll(ofSeconds(1)).asScala.toSeq


            for (records <- Stream.continually(reassign.andThen(nextRecords)(getNewTopics())).takeWhile(_ => !closed))
                obs onNext (records map buildBinaryRecord)

            obs onComplete()
        })
    }

    override def readTopic(topic: String, limitPerPartition: Long)(executionMonitor: Option[Subject[String]], stop: Option[Observable[Unit]]): Observable[Seq[BinaryTopicRecord]] = {
        println("read topics")
        val partitions: Seq[PartitionInfo] = consumer.listTopics().asScala.find(_._1 == topic).get._2.asScala
        val topicPartitions = partitions.map(info => new TopicPartition(info.topic(), info.partition()))

        val beginnings = consumer.beginningOffsets(topicPartitions.asJava).asScala.toMap
        val endings = consumer.endOffsets(topicPartitions.asJava).asScala.toSeq
        val position = (p: TopicPartition, e: Long) => if (limitPerPartition < 0L) beginnings(p).toLong else math.max(e - limitPerPartition, beginnings(p))
        val positions: Seq[(TopicPartition, Long)] = endings.map(x => x._1 -> position(x._1, x._2))

        val totalCount = endings.map(x => x._2 - beginnings(x._1)).sum
        consumer.assign(topicPartitions.asJava)
        for ((partition, position) <- positions) consumer.seek(partition, position)

        for (mon <- executionMonitor) mon onNext ("Total to read " + totalCount)

        var read = 0
        var stopped = false
        for (stopAction <- stop; _ <- stopAction) stopped = true
        Observable.create(obs => operation {
            for (recs <- Stream.continually(consumer.poll(ofSeconds(3))).takeWhile(!_.isEmpty && !stopped)) {
                val records = recs.asScala.toSeq
                read += records.size
                for (mon <- executionMonitor) mon onNext ("Read " + read + " of " + totalCount)
                obs onNext (records map buildBinaryRecord)
            }
            for (mon <- executionMonitor) mon onNext ("Completed read " + read + " of " + totalCount)
            obs onComplete()
        })
    }

    private def operation[R](f: => R): R = {
        if (!opSemaphore.tryAcquire()) throw new Exception("Operation within execution")
        else try f finally opSemaphore.release()
    }

    override def close(): Unit = {
        println("close")
        closed = true
        opSemaphore.acquire()
        consumer.close()
    }

    override def queryTopicPartitions(topic: String): Seq[Int] = {
        println("query topic partitions")
        consumer.partitionsFor(topic).asScala.map(_.partition()).sorted
    }

}

class KafkaProducerConnection(host: String,
                              groupName: String) extends ProducerConnection {

    private val producerProps = Map(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> host,
        ProducerConfig.ACKS_CONFIG -> "all",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
        ProducerConfig.PARTITIONER_CLASS_CONFIG -> classOf[DelegatingPartitioner].getName)

    private val producer = new KafkaProducer[Array[Byte], Array[Byte]]((producerProps: Map[String, Object]).asJava)

    override def send(topic: String, key: Array[Byte], value: Array[Byte], partition: PartitionerMode): Unit = {
        try {
        println("Try to send message!")
        CustomPartitioner.partitionMode = partition
        producer.send(new ProducerRecord(topic, key, value))
        } catch {
            case e: Exception => e.printStackTrace()
        }
    }

    override def close(): Unit = producer.close()

}

object CustomPartitioner {


    private val defaultPartitioner = new org.apache.kafka.clients.producer.internals.DefaultPartitioner()

    var partitionMode: PartitionerMode = _

    class DelegatingPartitioner extends Partitioner {

        override def partition(topic: String, key: Any, keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte], cluster: Cluster): Int = partitionMode match {
            case DefaultPartitioner => defaultPartitioner.partition(topic, key, keyBytes, value, valueBytes, cluster)
            case ExactPartition(partition) => partition
        }

        override def close(): Unit = Unit

        override def configure(configs: util.Map[String, _]): Unit = Unit
    }

}


