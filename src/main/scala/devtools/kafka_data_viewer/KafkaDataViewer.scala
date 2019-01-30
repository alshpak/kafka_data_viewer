package devtools.kafka_data_viewer

import java.io.{File, FileInputStream, FileOutputStream, FileWriter}
import java.net.URI
import java.time.Instant
import java.util

import devtools.kafka_data_viewer.KafkaConnTopicsInfo._
import devtools.kafka_data_viewer.KafkaDataViewer.ConnectionDefinition
import devtools.kafka_data_viewer.kafkaconn.Connector.{ConsumerConnection, ProducerConnection, TopicsWithSizes}
import devtools.kafka_data_viewer.kafkaconn.KafkaConnector
import devtools.kafka_data_viewer.kafkaconn.MessageFormats.{AvroMessage, MessageType, StringMessage, ZipMessage}
import devtools.kafka_data_viewer.ui.KafkaToolUpdate.UpdateUrl
import devtools.kafka_data_viewer.ui._
import devtools.lib.rxext.ListChangeOps.{AddItems, ListChangeOp, RemoveItemObjs, SetList}
import devtools.lib.rxext.ObservableSeqExt._
import devtools.lib.rxext.Subject.{behaviorSubject, publishSubject}
import devtools.lib.rxext.{BehaviorSubject, Observable, Subject}
import devtools.lib.rxui.FxRender.DefaultFxRenderes
import devtools.lib.rxui.UiImplicits._
import devtools.lib.rxui._
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
                             avroRegistires: BehaviorSubject[Seq[String]]
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

        val connector = new KafkaConnector(
            host = connDef.kafkaHost,
            groupName = groupName)

        ConnectionsSet(logging = connector.connectConsumer(), master = connector.connectConsumer(), producer = connector.connectProducer())
    }


    private val connectOps = behaviorSubject[ListChangeOp[ConnHandle]](SetList[ConnHandle](Seq()))
    private val connected: Observable[Seq[ConnHandle]] = connectOps.fromListOps()

    private val onConnect = publishSubject[ConnectionDefinition]()
    private val onClose: Subject[ConnHandle] = publishSubject()

    private class ConnHandle(val connDef: ConnectionDefinition,
                             val connSet: ConnectionsSet,
                             val initTopicsAndSizes: TopicsWithSizes,
                             val closeHndl: BehaviorSubject[Boolean] = behaviorSubject(),
                             val topicSettingsHndl: BehaviorSubject[Seq[(String, MessageType)]]) {
        override def equals(obj: Any): Boolean = obj match {
            case x: ConnHandle if x.connDef.name == connDef.name => true
            case _ => false
        }
    }

    for ((connectTo, connectedList) <- onConnect.withLatestFrom(connected); if !connectedList.exists(connectTo ==)) {
        val connectionSubject = publishSubject[Option[ConnHandle]]()
        val cancelConnection = publishSubject[Unit]()
        val topicSettings = behaviorSubject(connectTo.topicSettings)
        val connThread = new Thread(() =>
            try {
                val connSet = connectToServices(connectTo)
                val topicsAndSizes = connSet.master.queryTopicsWithSizes()

                connectionSubject <<
                        Some(new ConnHandle(connDef = connectTo,
                            connSet = connSet,
                            initTopicsAndSizes = topicsAndSizes,
                            topicSettingsHndl = topicSettings
                        ))
            } catch {
                case _: InterruptException => println("Connection is interrupted")
                case e: Exception => e.printStackTrace()
            })
        connThread.start()
        for (_ <- cancelConnection) {connectionSubject << None; connectionSubject onComplete(); connThread.interrupt() }

        val connectionDone = connectionSubject.observeOn(uiRenderer.uiScheduler())

        for (connectResult <- connectionDone; connHndl <- connectResult) {
            connectOps << AddItems(Seq(connHndl))
            for (topicSettings <- topicSettings) {
                connections << connections.value.map(connDef => if (connDef.name == connectTo.name) connDef.copy(topicSettings = topicSettings) else connDef)
            }
        }

        uiRenderer.runModal(
            content = UiPanel("", Grid("cols 1"), items = Seq(
                UiLabel(text = "Connecting to " + connectTo.kafkaHost),
                UiButton(text = "Cancel", onAction = cancelConnection)
            )),
            hideTitle = true,
            close = connectionDone.map[Any](_ => Unit).asPublishSubject
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
                UiTabPanelExt[ConnHandle](tabs = connectOps,
                    tab = (itemHndl: ConnHandle) => UiTab(
                        label = itemHndl.connDef.kafkaHost,
                        content = new KafkaConnectionPane("",
                            connDef = itemHndl.connDef,
                            loggingConsumer = itemHndl.connSet.logging,
                            masterConsumer = itemHndl.connSet.master,
                            producer = itemHndl.connSet.producer,
                            initTopicsAndSizes = itemHndl.initTopicsAndSizes,
                            filters = filters,
                            closed = itemHndl.closeHndl.value,
                            avroRegistires = avroRegistires,
                            topicToType = itemHndl.topicSettingsHndl)),
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

object KafkaConnTopicsInfo {
    type TopicInfoRec = (String, Long, Observable[MessageType])

    case class KafkaConnTopicsData(
                                          topicsList: Observable[Seq[TopicInfoRec]],
                                          messageTypes: Observable[Seq[MessageType]],
                                          onApplyMessageType: ((Seq[String], MessageType)) => Unit,
                                          onManageMessageTypes: () => Unit,
                                          onRequestRefresh: () => Unit)

}

case class AvroRegistriesManagement(
                                           registry: Observable[Seq[(String, Boolean)]],
                                           onAddNewServer: String => Unit,
                                           onRemoveServer: String => Unit)

class KafkaManageMessageTypesPane(val layoutData: String = "",
                                  avroRegistriesManagement: AvroRegistriesManagement
                                 )(implicit uiRenderer: UiRenderer) extends UiComponent {

    class ManageAvroConnectionsPane(val layoutData: String = "") extends UiComponent {

        type TableItem = (String, Boolean)

        private val selection = publishSubject[Seq[TableItem]]()

        private def addNewServer(): Unit =
            for (result <- serverValuePopup("")) avroRegistriesManagement.onAddNewServer(result)

        private def serverValuePopup(server: String): Option[String] = {
            var result: Option[String] = None
            val serverField = behaviorSubject(server)
            val onOk = publishSubject[Unit]()
            for (_ <- onOk) result = Some(serverField.value)
            val onClose = publishSubject[Unit]()
            onClose <<< onOk

            class ChangeServerPane(val layoutData: String = "") extends UiComponent {

                override def content(): UiWidget = UiPanel(layoutData, Grid("cols 2"), items = Seq(
                    UiLabel(text = "Avro Server"),
                    UiText("growx", text = serverField),
                    UiPanel("growx, colspan 2", Grid("cols 2"), items = Seq(
                        UiButton(text = "Ok", onAction = onOk),
                        UiButton(text = "Cancel", onAction = onClose)
                    ))
                ))
            }

            uiRenderer.runModal(new ChangeServerPane(), close = onClose)
            result
        }

        private val onRemove = publishSubject[Unit]()
        for ((_, selection) <- onRemove.withLatestFrom(selection); item <- selection.headOption; if !item._2) avroRegistriesManagement.onRemoveServer(item._1)

        override def content(): UiWidget = UiPanel(layoutData, Grid("cols 1"), items = Seq(
            UiLabel(text = "The list of Avro servers"),
            UiTable[TableItem]("grow",
                items = avroRegistriesManagement.registry.map(SetList(_)),
                columns = Seq[UiColumn[TableItem]](
                    UiColumn(title = "Registry", _._1),
                    UiColumn(title = "In Use", _._2.toString)),
                selection = selection,
                multiSelect = false
            ),
            UiPanel("growx", Grid("cols 2"), items = Seq(
                UiButton(text = "Add", onAction = addNewServer),
                UiButton(text = "Remove", onAction = onRemove)
            ))
        ))
    }

    override def content(): UiWidget = new ManageAvroConnectionsPane(layoutData)
}

class KafkaConnectionPane(val layoutData: String = "",
                          connDef: ConnectionDefinition,
                          loggingConsumer: ConsumerConnection,
                          masterConsumer: ConsumerConnection,
                          producer: ProducerConnection,
                          initTopicsAndSizes: TopicsWithSizes,
                          filters: BehaviorSubject[Seq[(String, Seq[String])]],
                          topicToType: BehaviorSubject[Seq[(String, MessageType)]],
                          avroRegistires: BehaviorSubject[Seq[String]],
                          closed: => Boolean)(implicit uiRenderer: UiRenderer) extends UiComponent {

    type TopicName = String
    private val msgEncoders: String => MessageType = topic => StringMessage
    private val typesRegistry = new TypesRegistry(topicToType, avroRegistires)

    private val refreshTopics: Subject[Unit] = behaviorSubject(Unit)

    private val topicsList: Observable[Seq[TopicInfoRec]] = refreshTopics
            .map(_ => masterConsumer.queryTopicsWithSizes())
            .mapSeq { case (topic, size) => (topic, size, topicToType.map(mappings => mappings.find(_._1 == topic).map(_._2).getOrElse(StringMessage))) }

    private val messageTypes: Subject[Seq[MessageType]] = behaviorSubject(
        StringMessage +: ZipMessage +: avroRegistires.value.map(AvroMessage)
    )

    private val applyMessageTypes = publishSubject[(Seq[TopicName], MessageType)]()
    for ((topics, typeToApply) <- applyMessageTypes)
        topicToType << (topicToType.value.filterNot { case (topic, _) => topics.contains(topic) } ++ topics.map(_ -> typeToApply))

    for (debug <- applyMessageTypes) println("Was attempt to change message types " + debug)

    private def manageMessageTypes(): Unit = {

        val registryMappings = avroRegistires.withLatestFrom(topicToType)
                .map { case (registries, mappings) =>
                    registries.map(registry => registry -> mappings.find(mapping => mapping._2 match {case AvroMessage(srv) if registry == srv => true; case _ => false}))
                }
                .mapSeq { case (registry, hasMappingOpt) => registry -> hasMappingOpt.isDefined }
        val mgmt = AvroRegistriesManagement(
            registry = registryMappings,
            onAddNewServer = srv => avroRegistires << avroRegistires.value :+ srv,
            onRemoveServer = srv => avroRegistires << avroRegistires.value.filterNot(srv ==)
        )
        uiRenderer.runModal(new KafkaManageMessageTypesPane(avroRegistriesManagement = mgmt))
    }

    private val topicsData = KafkaConnTopicsData(
        topicsList = topicsList,
        messageTypes = messageTypes,
        onApplyMessageType = applyMessageTypes <<,
        onManageMessageTypes = manageMessageTypes,
        onRequestRefresh = refreshTopics
    )

    override def content(): UiWidget = UiPanel("", Grid(), items = Seq(
        UiTabPanel("grow", tabs = Seq(
            UiTab(label = "Logging", content = new LoggingPane("",
                loggingConsumer = loggingConsumer,
                filters = filters,
                topicsData = topicsData,
                typesRegistry = typesRegistry,
                closed = closed)),
            UiTab(label = "Read Topic", content = new ReadTopicPane("",
                consumer = masterConsumer,
                topicsData = topicsData,
                typesRegistry = typesRegistry,
                closed)),
            UiTab(label = "Produce message", content = new ProduceMessagePane("",
                consumer = masterConsumer,
                producer = producer,
                topics = initTopicsAndSizes,
                msgEncoders = msgEncoders)),
            UiTab(label = "Consumers Info", content = new ConsumersInfoPane("", connDef)),
        ))
    ))
}

class UpdateInfoPane(val layoutData: String = "", val updateState: (AppUpdateState, UpdateUrl), onClose: Subject[Unit]) extends UiComponent {
    private val openUpdate = publishSubject[Unit]()
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

    case class TopicRecord(partition: Int, offset: Long, time: Instant, topic: String, key: String, value: String, msgtype: MessageType = StringMessage)

    case class ConnectionDefinition(name: String, kafkaHost: String, zoo: String, topicSettings: Seq[(String, MessageType)] = Seq())

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
            settings.get("connections").asInstanceOf[java.util.List[java.util.Map[String, Object]]].asScala
                    .map(condef => ConnectionDefinition(
                        name = condef.getOrDefault("name", condef.get("host")).asInstanceOf[String],
                        kafkaHost = condef.get("host").asInstanceOf[String],
                        zoo = condef.get("zoo").asInstanceOf[String],
                        topicSettings =
                                Option(condef.get("topicsettings")).getOrElse(new util.ArrayList()).asInstanceOf[util.List[String]].asScala
                                        .map(topicDefStr => topicDefStr.split(":") match {
                                            case Array(topic, "String") => topic -> StringMessage
                                            case Array(topic, "GZIP") => topic -> ZipMessage
                                            case Array(topic, "AVRO", avroHost) => topic -> AvroMessage(avroHost)
                                            case _ => "unknown" -> StringMessage
                                        })
                    )))

        for (connections <- connections) {
            settings.put("connections", connections.map(con => Map(
                "name" -> con.name,
                "host" -> con.kafkaHost,
                "zoo" -> con.zoo,
                "topicsettings" -> con.topicSettings.map {
                    case (topic, StringMessage) => topic + ":String"
                    case (topic, ZipMessage) => topic + ":GZIP"
                    case (topic, AvroMessage(avroHost)) => topic + ":AVRO:" + avroHost
                    case _ => /* JDD */ throw new IllegalStateException("not supported serialized type")
                }.toList.asJava
            ).asJava).toList.asJava)
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

        val avroRegistires = behaviorSubject[Seq[String]](
            settings.getOrDefault("avro_registries", new util.ArrayList[String]()).asInstanceOf[util.List[String]].asScala
        )
        for (avroRegistires <- avroRegistires) {
            settings.put("avro_registries", avroRegistires.toList.asJava)
            withres(new FileWriter(appProps))(settingsYaml.dump(settings, _))
        }

        DefaultFxRenderes.runApp(root = new KafkaDataViewerAppPane(
            updateState = KafkaToolUpdate.retrieveUpdateState(),
            connections = connections,
            filters = filters,
            groupName = groupName,
            avroRegistires = avroRegistires
        ))

    }

}
