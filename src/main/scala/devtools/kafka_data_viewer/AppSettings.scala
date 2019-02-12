package devtools.kafka_data_viewer

import java.io.{File, FileInputStream, FileWriter}
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.yaml.snakeyaml.{DumperOptions, Yaml}

import devtools.kafka_data_viewer.KafkaDataViewer.{ConnectionDefinition, ProducerSettings, _}
import devtools.kafka_data_viewer.NodeObservableLinking._
import devtools.kafka_data_viewer.ResourceUtility._
import devtools.kafka_data_viewer.TypeSerializers.TypeSerializer
import devtools.kafka_data_viewer.kafkaconn.MessageFormats.{AvroMessage, MessageType, StringMessage, ZipMessage}
import devtools.lib.rxext.BehaviorSubject
import devtools.lib.rxext.Subject.behaviorSubject
import devtools.lib.rxui.DisposeStore

class AppSettings(
                         val connections: BehaviorSubject[Seq[ConnectionDefinition]],
                         val filters: BehaviorSubject[Seq[FilterData]])

object AppSettings {

    def connect(): AppSettings = {

        val appPropsFile = new File("application.setting.yml")
        if (!appPropsFile.exists()) {
            appPropsFile.createNewFile()
        }

        val dumperOptions = new DumperOptions()
        dumperOptions.setPrettyFlow(true)
        dumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
        val settingsYaml = new Yaml(dumperOptions)
        val settingsRaw = withres(new FileInputStream(appPropsFile))(res => settingsYaml.load[java.util.Map[String, Object]](res))
        val settings = if (settingsRaw == null) new java.util.HashMap[String, Object]() else settingsRaw

        val root: Node = new Node(settings, onChange = withres(new FileWriter(appPropsFile))(settingsYaml.dump(settings, _)))

        val connections = behaviorSubject[Seq[ConnectionDefinition]]()

        import TypeSerializers._
        root.listNodes[ConnectionDefinition]("connections", connections, ConnectionDefinition(), (item, node) => {
            node.property("name", item.name)
            node.property("kafkaHost", item.kafkaHost)
            node.listStrings("avroRegistries", item.avroRegistries)
            node.listCustom("topicsSettings", item.topicSettings)
            node.listNodes[ProducerSettings]("producers", item.producers, ProducerSettings(), (item, node) => {
                node.property("topic", item.topic)
                node.propertyCustom("custom", item.custom)
                node.propertyCustom("messageType", item.msgType)
                node.propertyCustom("partition", item.partition)
                node.property("key", item.key)
                node.property("value", item.value)
                node.propertyCustom("order", item.order)
            })
        })

        val filters = behaviorSubject[Seq[FilterData]](Seq())

        root.listNodes[FilterData]("filters", filters, (behaviorSubject(), behaviorSubject()), (item, node) => {
            node.property("filter", item._1)
            node.listStrings("topics", item._2)
        })

        NodeObservableLinking.initialize = false

        new AppSettings(connections, filters)
    }
}

object TypeSerializers {

    trait TypeSerializer[T] {

        def serialize(t: T): String

        def deserialize(s: String): T
    }

    implicit val msgTypeType: TypeSerializer[MessageType] = new TypeSerializer[MessageType] {
        private val stringType: String = "String"
        private val zipType: String = "GZIP"
        private val avroType: String = "AVRO"

        override def serialize(t: MessageType): String = t match {
            case StringMessage => stringType
            case ZipMessage => zipType
            case AvroMessage(avroHost) => s"$avroType:$avroHost"
        }

        override def deserialize(s: String): MessageType = s match {
            case `stringType` => StringMessage
            case `zipType` => ZipMessage
            case avro if avro startsWith s"$avroType:" => AvroMessage(s.substring(avroType.length + 1))
            case _ => throw new IllegalStateException("not supported serialized type")
        }
    }

    implicit val topicToMsgType: TypeSerializer[TopicToMessageType] = new TypeSerializer[TopicToMessageType]() {

        override def serialize(t: TopicToMessageType): String = t._1 + ":" + msgTypeType.serialize(t._2)

        override def deserialize(s: String): TopicToMessageType = {
            val idx = s.indexOf(':')
            s.substring(0, idx) -> msgTypeType.deserialize(s.substring(idx + 1))
        }
    }

    implicit val booleanType: TypeSerializer[Boolean] = new TypeSerializer[Boolean] {

        override def serialize(t: Boolean): String = t.toString

        override def deserialize(s: String): Boolean = "true" == s
    }

    implicit val optionalIntType: TypeSerializer[Option[Int]] = new TypeSerializer[Option[Int]] {

        override def serialize(t: Option[Int]): String = t.map(_.toString).getOrElse("-")

        override def deserialize(s: String): Option[Int] = if (s == null || s == "-") None else Some(s.toInt)
    }

    implicit val optionalMessageTypeType: TypeSerializer[Option[MessageType]] = new TypeSerializer[Option[MessageType]] {

        override def serialize(t: Option[MessageType]): String = t.map(msgTypeType.serialize).getOrElse("-")

        override def deserialize(s: String): Option[MessageType] = if (s == null || s == "-") None else Some(msgTypeType.deserialize(s))
    }

    implicit val intType: TypeSerializer[Int] = new TypeSerializer[Int] {

        override def serialize(t: Int): String = t.toString

        override def deserialize(s: String): Int = if (s == null) -1 else s.toInt
    }
}

object NodeObservableLinking {

    val $ = new DisposeStore()
    var initialize = true

    class Node(root: java.util.Map[String, Object], onChange: => Unit) {

        def listNodes[T](nodeName: String, items: BehaviorSubject[Seq[T]], creator: => T, bind: (T, Node) => Unit): Unit = {
            root.putIfAbsent(nodeName, new util.ArrayList[util.Map[String, Object]]())
            val contents = root.get(nodeName).asInstanceOf[util.ArrayList[util.Map[String, Object]]]
            val prevItems = mutable.ArrayBuffer[(T, util.Map[String, Object])]()
            for (content <- contents.asScala) {
                val item = creator
                val thisNode = new Node(content, onChange)
                bind(item, thisNode)
                prevItems += item -> content
            }
            items << prevItems.map(_._1)

            for (items <- $(items)) {
                val newItems = items diff prevItems.map(_._1)
                val removedItems = prevItems.filterNot(x => items.contains(x._1))
                for (item <- newItems) {
                    val content = new util.HashMap[String, Object]()
                    val node = new Node(content, onChange)
                    bind(item, node)
                    contents.add(content)
                    prevItems += item -> content
                }
                for (item <- removedItems) {
                    contents.remove(item._2)
                    prevItems -= item
                }
                onChange
            }
        }

        def property(nodeName: String, prop: BehaviorSubject[String], defaultValue: String = ""): Unit = {
            if (initialize) prop << root.getOrDefault(nodeName, defaultValue).toString
            for (change <- $(prop)) {root.put(nodeName, change); onChange }
        }

        def propertyCustom[T](nodeName: String, prop: BehaviorSubject[T])(implicit converter: TypeSerializer[T]): Unit = {
            if (initialize) prop << converter.deserialize(root.get(nodeName).asInstanceOf[String])
            for (change <- $(prop)) {root.put(nodeName, converter.serialize(change)); onChange }
        }

        def listStrings(nodeName: String, prop: BehaviorSubject[Seq[String]]): Unit = {
            if (initialize) {
                val initial = root.get(nodeName).asInstanceOf[util.List[String]].asScala
                prop << Option(initial).getOrElse(Seq())
            }
            for (change <- $(prop)) {root.put(nodeName, change.asJava); onChange }
        }

        def listCustom[T](nodeName: String, prop: BehaviorSubject[Seq[T]])(implicit converter: TypeSerializer[T]): Unit = {
            if (initialize) {
                val initial = root.get(nodeName).asInstanceOf[util.List[String]].asScala
                prop << Option(initial).getOrElse(Seq()).map(converter.deserialize)
            }
            for (change <- $(prop)) {root.put(nodeName, change.map(converter.serialize).asJava); onChange }
        }
    }

}

object ResourceUtility {
    def withres[T <: AutoCloseable, R](t: T)(f: T => R): R = try f(t) finally t.close()
}