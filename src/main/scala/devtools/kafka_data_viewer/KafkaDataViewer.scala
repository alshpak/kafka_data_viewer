package devtools.kafka_data_viewer

import java.io.{File, FileInputStream, FileOutputStream, FileWriter}
import java.net.URI
import java.util

import devtools.kafka_data_viewer.KafkaDataViewer.ConnectionDefinition
import devtools.kafka_data_viewer.kafkaconn.KafkaConnector
import devtools.kafka_data_viewer.kafkaconn.KafkaConnector.{ConsumerConnection, ProducerConnection, TopicRecord, TopicsWithSizes}
import devtools.kafka_data_viewer.ui.KafkaToolUpdate.UpdateUrl
import devtools.kafka_data_viewer.ui._
import devtools.lib.rxext.ListChangeOps.{AddItems, ListChangeOp, RemoveItemObjs, SetList}
import devtools.lib.rxext.Subject.{behaviorSubject, publishSubject}
import devtools.lib.rxext.{BehaviorSubject, Observable, Subject}
import devtools.lib.rxui.FxRender.DefaultFxRenderes
import devtools.lib.rxui.UiImplicits._
import devtools.lib.rxui._
import io.reactivex.Scheduler
import io.reactivex.schedulers.Schedulers
import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.kafka.common.errors.InterruptException
import org.yaml.snakeyaml.{DumperOptions, Yaml}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.postfixOps

class KafkaDataViewerAppPane(val layoutData: String = "",
                             updateState: (AppUpdateState, UpdateUrl),
                             connections: BehaviorSubject[Seq[ConnectionDefinition]],
                             filters: BehaviorSubject[Seq[(String, Seq[String])]],
                             groupName: String,
                             binaryQueues: Seq[String],
                             registryQueues: Seq[String]
                            )(implicit uiRenderer: UiRenderer) extends UiComponent {


    case class ConnectionsSet(logging: ConsumerConnection, master: ConsumerConnection, producer: ProducerConnection)

    private def connectToServices(connDef: ConnectionDefinition) = {

        def queueResolverBySet(queuesDef: Seq[String]): String => Boolean = {
            val cache = mutable.HashMap[String, Boolean]()
            topic => {
                if (!cache.contains(topic))
                    cache(topic) = queuesDef.exists(topicDef => topic == topicDef || topicDef.endsWith("*") && topic.startsWith(topicDef.substring(0, topicDef.length - 1)))
                cache(topic)
            }
        }

        val connector = KafkaConnector.connect(
            host = connDef.kafkaHost,
            avroServer = connDef.avroHost,
            groupName = groupName,
            binaryQueue = queueResolverBySet(binaryQueues),
            registryQueue = queueResolverBySet(registryQueues))

        ConnectionsSet(logging = connector.connectConsumer(), master = connector.connectConsumer(), producer = connector.connectProducer())
    }


    private val connectOps = behaviorSubject[ListChangeOp[ConnHandle]](SetList[ConnHandle](Seq()))
    private val connected: Observable[Seq[ConnHandle]] = connectOps.fromListOps()

    private val onConnect = publishSubject[ConnectionDefinition]()
    private val onClose: Subject[ConnHandle] = publishSubject()

    private case class ConnHandle(connDef: ConnectionDefinition, connSet: ConnectionsSet, initTopicsAndSizes: TopicsWithSizes, closeHndl: BehaviorSubject[Boolean] = behaviorSubject(false))

    for ((connectTo, connectedList) <- onConnect.withLatestFrom(connected); if !connectedList.exists(connectTo ==)) {
        val connectionSubject = publishSubject[Option[ConnHandle]]()
        val cancelConnection = publishSubject[Any]()
        val connThread = new Thread(() =>
            try {
                val connSet = connectToServices(connectTo)
                val topicsAndSizes = connSet.master.queryTopicsWithSizes()
                connectionSubject <<
                        Some(ConnHandle(connDef = connectTo, connSet = connSet, initTopicsAndSizes = topicsAndSizes))
            } catch {
                case _: InterruptException => println("Connection is interrupted")
                case e: Exception => e.printStackTrace()
            })
        connThread.start()
        for (_ <- cancelConnection) {connectionSubject << None; connectionSubject onComplete(); connThread.interrupt() }

        val connectionDone = connectionSubject.observeOn(uiRenderer.uiScheduler())

        for (connectResult <- connectionDone; connHndl <- connectResult)
            connectOps << AddItems(Seq(connHndl))

        uiRenderer.runModal(
            content = UiPanel("", Grid("cols 1"), items = Seq(
                UiLabel(text = "Connecting to " + connectTo.kafkaHost),
                UiButton(text = "Cancel", onAction = cancelConnection)
            )),
            hideTitle = true,
            close = connectionDone.map[Any](_ => Unit).asSubject
        )
    }

    for (connHandle <- onClose) {
        connHandle.closeHndl onNext true
        connectOps << RemoveItemObjs(Seq(connHandle))
    }

    for (connHandle <- onClose.subscribeOn(Schedulers.newThread())) {
        connHandle.connSet.master.close()
        connHandle.connSet.logging.close()
        connHandle.connSet.producer.close()
    }


    override def content(): UiWidget = UiPanel("", Grid(), items = Seq(
        UiSplitPane("grow", orientation = UiHoriz, proportion = 15, els = (
                UiTabPanel(tabs = Seq(
                    UiTab(label = "Connections List", content = new KafkaConnectionsListPane(connections = connections, onConnect = onConnect)))),
                UiTabPanelListOps[ConnHandle](tabs = connectOps,
                    tab = (itemHndl: ConnHandle) => UiTab(
                        label = itemHndl.connDef.kafkaHost,
                        content = new KafkaConnectionPane("",
                            connDef = itemHndl.connDef,
                            loggingConsumer = itemHndl.connSet.logging,
                            masterConsumer = itemHndl.connSet.master,
                            producer = itemHndl.connSet.producer,
                            initTopicsAndSizes = itemHndl.initTopicsAndSizes,
                            filters = filters,
                            closed = itemHndl.closeHndl.value)),
                    closeable = true,
                    onClose = el => onClose onNext el,
                    moveTabs = true
                )
        ))
    ))

    //        override def init(): Unit = {
    //            if (!updateState._1.isInstanceOf[AppUpdateCurrent]) {
    //                val closeHandle = PublishSubject.create[Any]()
    //                ModalWindow.run(new UpdateInfoWindow(updateState, closeHandle), close = closeHandle)
    //            }
    //        }
}


class KafkaConnectionPane(val layoutData: String = "",
                          connDef: ConnectionDefinition,
                          loggingConsumer: ConsumerConnection,
                          masterConsumer: ConsumerConnection,
                          producer: ProducerConnection,
                          initTopicsAndSizes: TopicsWithSizes,
                          filters: BehaviorSubject[Seq[(String, Seq[String])]],
                          closed: => Boolean)(implicit uiRenderer: UiRenderer) extends UiComponent {

    println("construct a connection pane")

    override def content(): UiWidget = UiPanel("", Grid(), items = Seq(
        UiTabPanel("grow", tabs = Seq(
            UiTab(label = "Logging", content = new LoggingPane("", loggingConsumer, filters, initTopicsAndSizes, closed)),
            UiTab(label = "Read Topic", content = new ReadTopicPane("", masterConsumer, initTopicsAndSizes, closed)),
            UiTab(label = "Produce message", content = new ProduceMessagePane("", producer, initTopicsAndSizes)),
            UiTab(label = "Consumers Info", content = new ConsumersInfoPane("", connDef)),
        ))
    ))
}

class UpdateInfoPane(val layoutData: String = "", val updateState: (AppUpdateState, UpdateUrl), onClose: Subject[Any]) extends UiComponent {
    private val openUpdate = publishSubject[Any]()
    for (_ <- openUpdate) java.awt.Desktop.getDesktop.browse(new URI(updateState._2))

    override def content(): UiWidget = UiPanel(layout = Grid(), items = Seq(
        UiLabel(text = "Update state is"),
        UiLabel(text = updateState._1 match {
            case AppUpdateUnavailable(reason) => "Update not available : " + reason
            case AppUpdateRequired(version, newVersion) => s"Current version: $version; new version $newVersion is available"
        }),
        UiButton(text = "Click to open update site", onAction = openUpdate),
        UiButton(text = "Close", onAction = onClose)
    ))
}


object KafkaDataViewer {

    case class ConnectionDefinition(kafkaHost: String, avroHost: String, zoo: String)

    def isNumber(s: String): Boolean = try {s.toLong; true} catch {case _: Exception => false}

    def search(record: TopicRecord, text: String): Boolean = (record.topic.contains(text)
            || record.key != null && record.key.contains(text)
            || record.value != null && record.value.contains(text))


    def main(args: Array[String]): Unit = {

        def withres[T <: AutoCloseable, R](t: T)(f: T => R): R = try f(t) finally t.close()

        val options = new Options()
        options.addRequiredOption("n", "groupname", true, "Group name to connect to kafka server")

        val cli = new DefaultParser().parse(options, args)
        val groupName = cli.getOptionValue("n")

        val appProps = new File("application.yml")
        if (!appProps.exists()) {

            val appPropsRes = KafkaDataViewer.getClass.getClassLoader.getResourceAsStream(appProps.getName)
            withres(new FileOutputStream(appProps)) { out =>
                val buf = Array.ofDim[Byte](4096)
                Iterator continually (appPropsRes read buf) takeWhile (_ > -1) foreach (out.write(buf, 0, _))
            }
        }

        val dumperOptions = new DumperOptions()
        dumperOptions.setPrettyFlow(true)
        dumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
        val settingsYaml = new Yaml(dumperOptions)
        val settings = withres(new FileInputStream(appProps))(res => settingsYaml.load[java.util.Map[String, Object]](res))
        withres(new FileWriter(appProps))(settingsYaml.dump(settings, _))

        val connections = behaviorSubject[Seq[ConnectionDefinition]](
            settings.get("connections").asInstanceOf[java.util.List[java.util.Map[String, String]]].asScala
                    .map(condef => ConnectionDefinition(kafkaHost = condef.get("host"), avroHost = condef.get("avro"), zoo = condef.get("zoo"))))

        for (connections <- connections) {
            settings.put("connections", connections.map(con => Map("host" -> con.kafkaHost, "avro" -> con.avroHost, "zoo" -> con.zoo).asJava).toList.asJava)
            withres(new FileWriter(appProps))(settingsYaml.dump(settings, _))
        }

        val filters = behaviorSubject[Seq[(String, Seq[String])]](
            settings.getOrDefault("filters", new util.HashMap()).asInstanceOf[util.Map[String, util.List[String]]].asScala
                    .map { case (key, items) => key -> items.asScala }
                    .toSeq
                    .sortBy(_._1))

        for (filters <- filters) {
            settings.put("filters", filters.map { case (key, values) => key -> values.asJava }.toMap.asJava)
            withres(new FileWriter(appProps))(settingsYaml.dump(settings, _))
        }


        DefaultFxRenderes.runApp(root = new KafkaDataViewerAppPane(
            updateState = KafkaToolUpdate.retrieveUpdateState(),
            connections = connections,
            filters = filters,
            groupName = groupName,
            binaryQueues = settings.get("queues_settings").asInstanceOf[java.util.Map[String, java.util.List[String]]].get("binary").asScala,
            registryQueues = settings.get("queues_settings").asInstanceOf[java.util.Map[String, java.util.List[String]]].get("registry").asScala
        ))

    }

}
