package devtools.kafka_data_viewer.kafkaconn

import java.io.{BufferedReader, ByteArrayInputStream, InputStreamReader}
import java.time.Instant
import java.util
import java.util.concurrent.Semaphore
import java.util.zip.GZIPInputStream

import devtools.kafka_data_viewer.kafkaconn.KafkaConnector.{ConsumerConnection, ProducerConnection}
import devtools.lib.rxext.Observable.{empty, just}
import devtools.lib.rxext.{Observable, Subject}
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.generic.GenericData
import org.apache.avro.reflect.ReflectData
import org.apache.avro.{Conversions, Schema}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringSerializer}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import tech.allegro.schema.json2avro.converter.JsonAvroConverter

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object KafkaConnector {

    trait MessageType

    case object StringMessage extends MessageType

    case object ZipMessage extends MessageType

    case object AvroMessage extends MessageType

    case class TopicRecord(partition: Int, offset: Long, time: Instant, topic: String, key: String, value: String, msgtype: MessageType = StringMessage)

    def `with`[T, R](value: => T)(f: T => R): R = f(value)

    def withClosable[T <: AutoCloseable, R](f: () => T)(p: T => R): R = `with`(f())(res => try p(res) finally res.close())

    type TopicsWithSizes = Seq[(String, Long)]

    trait ConsumerConnection {
        def queryTopics(): Seq[String]

        def queryTopicsWithSizes(): TopicsWithSizes

        def readNextRecords()(executionMonitor: Option[Subject[String]] = None, stop: Option[Observable[Unit]] = None): Observable[Seq[TopicRecord]]

        def readTopicsContinually(topics: Observable[Seq[String]]): Observable[Seq[TopicRecord]]

        def readTopic(topic: String, limitPerPartition: Long)(executionMonitor: Option[Subject[String]] = None, stop: Option[Observable[Unit]] = None): Observable[Seq[TopicRecord]]

        def close(): Unit
    }

    trait ProducerConnection {

        def send(topic: String, key: String, message: String): Unit

        def close(): Unit

    }

    def stubConnector(): KafkaConnector = new KafkaConnector {

        override def connectConsumer(): ConsumerConnection = new ConsumerConnection {

            override def queryTopics(): Seq[String] = Seq("topic1", "topic2", "topic3")

            override def queryTopicsWithSizes(): Seq[(String, Long)] = queryTopics().zipWithIndex.map(x => x._1 -> x._2.toLong)

            override def readNextRecords()(executionMonitor: Option[Subject[String]], stop: Option[Observable[Unit]]): Observable[Seq[TopicRecord]] = empty()

            override def readTopicsContinually(topics: Observable[Seq[String]]): Observable[Seq[TopicRecord]] = {
                val publisher = Subject.publishSubject[Seq[TopicRecord]]()

                val currentTopics = Subject.behaviorSubject(Seq[String]())
                currentTopics <<< topics
                new Thread { () =>
                    publisher << (for (topic <- currentTopics.value; amount <- 0 to Random.nextInt(5)) yield
                        TopicRecord(partition = Random.nextInt(3), offset = 1, time = Instant.now(), topic = topic, key = "some key", value = "Some value", msgtype = StringMessage))
                    Thread.sleep(1000)
                }.start()

                publisher
            }

            override def readTopic(topic: String, limitPerPartition: Long)(executionMonitor: Option[Subject[String]], stop: Option[Observable[Unit]]): Observable[Seq[TopicRecord]] = {
                just(for {
                    idx <- 1.to(Random.nextInt(100))
                    rec = TopicRecord(partition = Random.nextInt(3), offset = 1, time = Instant.now(), topic = topic, key = "some key", value = "Some value", msgtype = StringMessage)
                } yield rec)
            }

            override def close(): Unit = Unit
        }

        override def connectProducer(): ProducerConnection = new ProducerConnection {
            override def send(topic: String, key: String, message: String): Unit = Unit

            override def close(): Unit = Unit
        }

        override def closeAll(): Unit = Unit
    }

    def connect(host: String,
                avroServer: String,
                groupName: String,
                binaryQueue: String => Boolean,
                registryQueue: String => Boolean): KafkaConnector = new KafkaConnector {

        private val consumers = ArrayBuffer[ConsumerConnection]()
        private val producers = ArrayBuffer[ProducerConnection]()

        override def connectConsumer(): ConsumerConnection = new ConsumerConnection {

            consumers += this

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

            println("Connect with " + (consumerProps : Map[String, Object] ).asJava.get("bootstrap.servers"))
            private val consumer = new KafkaConsumer[Array[Byte], Array[Byte]]((consumerProps: Map[String, Object]).asJava)

            private val schemaClient = Option(avroServer).map(host => {
                ReflectData.get().addLogicalTypeConversion(new Conversions.DecimalConversion())
                new CachedSchemaRegistryClient(host, 100)
            })
            private val registryDeser = schemaClient.map(client => new KafkaAvroDeserializer(client))
            //private val registrySer = schemaClient.map(client => new KafkaAvroSerializer(client))


            override def queryTopics(): Seq[String] = operation {
                consumer.listTopics().asScala.filterNot(_._1.startsWith("_")).keys.toSeq.sorted
            }

            override def queryTopicsWithSizes(): Seq[(String, Long)] = operation {
                val allTopics: mutable.Map[String, util.List[PartitionInfo]] = consumer.listTopics().asScala
                val topics = allTopics.filterNot(_._1.startsWith("_"))
                val topicPartitions: Seq[TopicPartition] = topics.values.flatMap(x => x.asScala)
                        .map(info => new TopicPartition(info.topic(), info.partition())).toSeq
                val beginnings = consumer.beginningOffsets(topicPartitions.asJava).asScala.toMap
                val endings = consumer.endOffsets(topicPartitions.asJava).asScala.toSeq
                val sizes = endings.map { case (ref, end) => ref -> (end - beginnings(ref)) }
                val result = sizes.foldLeft(Map[String, Long]())((acc, el) => acc + (el._1.topic() -> (el._2 + acc.getOrElse(el._1.topic(), 0L)))).toSeq
                result.sortBy(_._1)
            }

            override def readNextRecords()(executionMonitor: Option[Subject[String]], stop: Option[Observable[Unit]]): Observable[Seq[TopicRecord]] = {
                var read = 0
                var stopped = false
                for (stopAction <- stop; _ <- stopAction) stopped = true
                Observable.create(obs => operation {
                    for (recs <- Stream.continually(consumer.poll(3000)).takeWhile(!_.isEmpty && !stopped)) {
                        val records = recs.asScala.toSeq
                        read += records.size
                        for (mon <- executionMonitor) mon onNext ("Read " + read)
                        obs onNext (records map decodeMessage)
                    }
                    for (mon <- executionMonitor) mon onNext ("Completed read " + read)
                    obs onComplete()
                })
            }

            override def readTopicsContinually(topics: Observable[Seq[String]]): Observable[Seq[TopicRecord]] = {
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
                    //val sync = new Object()

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
                        else consumer.poll(1000).asScala.toSeq

                    for (records <- Stream.continually(reassign.andThen(nextRecords)(getNewTopics())).takeWhile(_ => !closed))
                        obs onNext (records map decodeMessage)

                })
            }

            override def readTopic(topic: String, limitPerPartition: Long)(executionMonitor: Option[Subject[String]], stop: Option[Observable[Unit]]): Observable[Seq[TopicRecord]] = {
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
                    for (recs <- Stream.continually(consumer.poll(3000)).takeWhile(!_.isEmpty && !stopped)) {
                        val records = recs.asScala.toSeq
                        read += records.size
                        for (mon <- executionMonitor) mon onNext ("Read " + read + " of " + totalCount)
                        obs onNext (records map decodeMessage)
                    }
                    for (mon <- executionMonitor) mon onNext ("Completed read " + read + " of " + totalCount)
                    obs onComplete()
                })
            }

            private def operation[R](f: => R): R = {
                if (!opSemaphore.tryAcquire()) throw new Exception("Operation within execution")
                else try f finally opSemaphore.release()
            }

            private def decodeMessage(record: ConsumerRecord[Array[Byte], Array[Byte]]): TopicRecord = {
                val (message, _) = record.topic() match {
                    case topic if binaryQueue(topic) =>
                        val message = ungzipString(record.value)
                        (message, s"Binary (raw size: ${record.value().length}; content size: ${message.length})")
                    case topic if registryQueue(topic) =>
                        (unpackRegistryMessage(topic, record.value()), "")
                    case _ => (new String(record.value, "UTF8"), "String")
                }
                //val maxLength = 1000
                val messageDisplay = message //if (message.length > maxLength) message.substring(0, maxLength) + "\nAnd " + (message.length - maxLength) + " more..." else message
                TopicRecord(
                    time = Instant.ofEpochMilli(record.timestamp()),
                    partition = record.partition(),
                    offset = record.offset(),
                    topic = record.topic(),
                    key = Option(record.key()).map(new String(_, "UTF8")).orNull,
                    value = messageDisplay)
            }

            private def ungzipString(gzipped: Array[Byte]) = {
                val isf = () => new BufferedReader(new InputStreamReader(
                    new GZIPInputStream(new ByteArrayInputStream(gzipped))))
                val readContent = (br: BufferedReader) => Stream.continually(br.readLine()).takeWhile(null !=).mkString("\n")
                withClosable(isf)(readContent)
            }

            private def unpackRegistryMessage(topic: String, content: Array[Byte]): String = {
                try {
                    val record = registryDeser.get.deserialize(topic, content).asInstanceOf[GenericData.Record]
                    val converter = new JsonAvroConverter()
                    val json = new String(converter.convertToJson(record), "UTF8")
                    json
                } catch {
                    case e: Exception =>
                        "Can not unpack " + e.toString
                }


            }

            override def close(): Unit = {
                closed = true
                opSemaphore.acquire()
                consumer.close()
            }

        }

        override def connectProducer(): ProducerConnection = new ProducerConnection {

            producers += this

            private val producerProps = Map(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> host,
                ProducerConfig.ACKS_CONFIG -> "all",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getName,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName)

            private val producer = new KafkaProducer[String, Array[Byte]]((producerProps: Map[String, Object]).asJava)

            private val schemaClient = Option(avroServer).map(host => {
                ReflectData.get().addLogicalTypeConversion(new Conversions.DecimalConversion())
                new CachedSchemaRegistryClient(host, 100)
            })
            //private val registryDeser = schemaClient.map(client => new KafkaAvroDeserializer(client))
            //private val registrySer = schemaClient.map(client => new KafkaAvroSerializer(client))

            override def send(topic: String, key: String, message: String): Unit = topic match {
                case _ if binaryQueue(topic) =>
                case _ if registryQueue(topic) =>
                    producer.send(new ProducerRecord[String, Array[Byte]](topic, key, encodeRegistryMessage(topic, message)))
                case _ =>
                    producer.send(new ProducerRecord[String, Array[Byte]](topic, key, message.getBytes("UTF8")))
            }

            override def close(): Unit = producer.close()

            def encodeRegistryMessage(topic: String, message: String): Array[Byte] = {
                /*        val json = new JSONObject(message)

                        def castElement(el: Object): Object = el match {
                            case el: JSONArray => jsonArrToList(el)
                            case el: JSONObject => jsonToMap(el)
                            case x => x
                        }

                        def jsonArrToList(jsonArr: JSONArray): java.util.List[Object] =
                            jsonArr.iterator().asScala.map(castElement).toList.asJava

                        def jsonToMap(json: JSONObject): GenericRecord = {
                            val data = json.keySet().asScala.map(k => (k, castElement(json.get(k)))).toMap
                            ???
                        }

                        val generic = castElement(json)

                        val schemaMeta = schemaClient.get.getLatestSchemaMetadata(topic)
                        schemaMeta.getSchema

                        val schema = new Schema.Parser().parse(schemaMeta.getSchema)

                        val encoderFactory = EncoderFactory.get

                        val outArray = new ByteArrayOutputStream()
                        outArray.write(0)
                        outArray.write(ByteBuffer.allocate(4).putInt(schemaMeta.getId).array)

                        val en = encoderFactory.directBinaryEncoder(outArray, null.asInstanceOf[BinaryEncoder])

                        val t = new GenericDatumWriter[Object](schema)
                        t.write(generic, en)

                        outArray.toByteArray*/


                val schemaMeta = schemaClient.get.getLatestSchemaMetadata(topic)
                val schema = new Schema.Parser().parse(schemaMeta.getSchema)

                val converter = new JsonAvroConverter()
                val record = converter.convertToGenericDataRecord(message.getBytes, schema)
                val bytes = new KafkaAvroSerializer(schemaClient.get).serialize(topic, record)
                bytes
            }
        }

        override def closeAll(): Unit = {
            consumers foreach (_.close())
            producers foreach (_.close())
        }
    }

}

trait KafkaConnector {

    def connectConsumer(): ConsumerConnection

    def connectProducer(): ProducerConnection

    def closeAll(): Unit

}

