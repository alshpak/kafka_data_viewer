package devtools.kafka_data_viewer

import java.io.{File, FileInputStream, FileWriter}
import java.util

import devtools.kafka_data_viewer.AppSettings.FilterData
import devtools.kafka_data_viewer.KafkaDataViewer.ConnectionDefinition
import devtools.kafka_data_viewer.kafkaconn.MessageFormats.{AvroMessage, MessageType, StringMessage, ZipMessage}
import devtools.lib.rxext.BehaviorSubject
import devtools.lib.rxext.Subject.behaviorSubject
import devtools.lib.rxui.DisposeStore
import org.yaml.snakeyaml.{DumperOptions, Yaml}

import scala.collection.JavaConverters._
import scala.collection.mutable

object AppSettings {

    type FilterData = (BehaviorSubject[String], BehaviorSubject[Seq[String]])

    trait TypeSerializer[T] {

        def serialize(t: T): String

        def deserialize(s: String): T
    }

    type TopicToMessageType = (String, MessageType)

    private implicit val topicToMsgType: TypeSerializer[TopicToMessageType] = new TypeSerializer[TopicToMessageType]() {
        override def serialize(t: TopicToMessageType): String = t match {
            case (topic, StringMessage) => topic + ":String"
            case (topic, ZipMessage) => topic + ":GZIP"
            case (topic, AvroMessage(avroHost)) => topic + ":AVRO:" + avroHost
            case _ => /* JDD */ throw new IllegalStateException("not supported serialized type")
        }

        override def deserialize(s: String): TopicToMessageType = s.split(":") match {
            case Array(topic, "String") => topic -> StringMessage
            case Array(topic, "GZIP") => topic -> ZipMessage
            case Array(topic, "AVRO", avroHost) => topic -> AvroMessage(avroHost)
            case _ => "unknown" -> StringMessage
        }
    }


    def withres[T <: AutoCloseable, R](t: T)(f: T => R): R = try f(t) finally t.close()


    def connect(defaultGroup: String): AppSettings = {

        val $  = new DisposeStore()

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

        val root: Node = new Node(settings, onChange = withres(new FileWriter(appPropsFile))(settingsYaml.dump(settings, _)))

        val connections = behaviorSubject[Seq[ConnectionDefinition]]()

        root.listNodes[ConnectionDefinition]("connections", connections, ConnectionDefinition(), (item, node) => {
            node.property("name", item.name)
            node.property("kafkaHost", item.kafkaHost)
            node.property("group", item.group, defaultGroup)
            node.listStrings("avroRegistries", item.avroRegistries)
            node.listCustom("topicsSettings", item.topicSettings)
        })

        val filters = behaviorSubject[Seq[FilterData]](Seq())

        root.listNodes[FilterData]("filters", filters, (behaviorSubject(), behaviorSubject()), (item, node) => {
            node.property("filter", item._1)
            node.listStrings("topics", item._2)
        })

        initialize = false

        new AppSettings(connections, filters)
    }

}

class AppSettings(
                         val connections: BehaviorSubject[Seq[ConnectionDefinition]],
                         val filters: BehaviorSubject[Seq[FilterData]]
                 )
