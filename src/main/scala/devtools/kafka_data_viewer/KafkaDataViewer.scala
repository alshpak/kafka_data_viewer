package devtools.kafka_data_viewer

import java.net.URI
import java.time.Instant

import devtools.kafka_data_viewer.AppSettings.FilterData
import devtools.kafka_data_viewer.KafkaConnTopicsInfo._
import devtools.kafka_data_viewer.KafkaDataViewer.ConnectionDefinition
import devtools.kafka_data_viewer.kafkaconn.Connector.{ConsumerConnection, ProducerConnection, TopicsWithSizes}
import devtools.kafka_data_viewer.kafkaconn.KafkaConnector
import devtools.kafka_data_viewer.kafkaconn.MessageFormats.{AvroMessage, MessageType, StringMessage, ZipMessage}
import devtools.kafka_data_viewer.ui.KafkaToolUpdate.UpdateUrl
import devtools.kafka_data_viewer.ui._
import devtools.lib.rxext.ListChangeOps.{AddItems, ListChangeOp, RemoveItemObjs, SetList}
import devtools.lib.rxext.Observable.{just, merge}
import devtools.lib.rxext.ObservableSeqExt._
import devtools.lib.rxext.Subject.{behaviorSubject, publishSubject}
import devtools.lib.rxext.{BehaviorSubject, Observable, Subject}
import devtools.lib.rxui.FxRender.DefaultFxRenderes
import devtools.lib.rxui.UiImplicits._
import devtools.lib.rxui._
import io.reactivex.schedulers.Schedulers
import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.kafka.common.errors.InterruptException

import scala.collection.mutable
import scala.language.postfixOps

class KafkaDataViewerAppPane(val layoutData: String = "",
                             updateState: (AppUpdateState, UpdateUrl),
                             connections: BehaviorSubject[Seq[ConnectionDefinition]],
                             filters: BehaviorSubject[Seq[FilterData]],
                             groupName: String
                            )(implicit uiRenderer: UiRenderer) extends UiComponent {


    case class ConnectionsSet(logging: ConsumerConnection, read: ConsumerConnection, master: ConsumerConnection, producer: ProducerConnection)

    private def connectToServices(connDef: ConnectionDefinition) = {
        val connector = new KafkaConnector(
            host = connDef.kafkaHost.value,
            groupName = groupName)

        ConnectionsSet(logging = connector.connectConsumer(), read = connector.connectConsumer(), master = connector.connectConsumer(), producer = connector.connectProducer())
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
        val connectionSubject = publishSubject[Option[Either[String, ConnHandle]]]()
        val cancelConnection = publishSubject[Unit]()
        val connThread = new Thread(() =>
            try {
                val connSet = connectToServices(connectTo)
                val topicsAndSizes = connSet.master.queryTopicsWithSizes()

                connectionSubject <<
                        Some(Right(new ConnHandle(connDef = connectTo,
                            connSet = connSet,
                            initTopicsAndSizes = topicsAndSizes,
                            topicSettingsHndl = connectTo.topicSettings
                        )))
            } catch {
                case _: InterruptException =>
                case e: Exception =>
                    println("Error on connection")
                    e.printStackTrace()
                    connectionSubject << Some(Left(e.getMessage))
            })
        connThread.start()
        for (_ <- cancelConnection) {connectionSubject << None; connectionSubject onComplete(); connThread.interrupt() }

        val connectionDone: Observable[Option[Either[String, ConnHandle]]] = connectionSubject.observeOn(uiRenderer.uiScheduler())

        for (connectResultOpt <- connectionDone; connectResult <- connectResultOpt) {
            connectResult match {
                case Right(connHndl) => connectOps << AddItems(Seq(connHndl))
                case Left(error) => uiRenderer.alert(ErrorAlert, "Connection in cancelled; " + error)
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
        println("Close attempt")
        connHandle.connSet.master.close()
        connHandle.connSet.logging.close()
        connHandle.connSet.read.close()
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
                            readDataConsumer = itemHndl.connSet.read,
                            masterConsumer = itemHndl.connSet.master,
                            producer = itemHndl.connSet.producer,
                            initTopicsAndSizes = itemHndl.initTopicsAndSizes,
                            filters = filters,
                            closed = itemHndl.closeHndl.value,
                            avroRegistires = itemHndl.connDef.avroRegistries,
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

    case class KafkaTopicsMgmt(
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
                UiButton(text = "Add", onAction = () => addNewServer()),
                UiButton(text = "Remove", onAction = onRemove)
            ))
        ))
    }

    override def content(): UiWidget = new ManageAvroConnectionsPane(layoutData)
}

class KafkaConnectionPane(val layoutData: String = "",
                          connDef: ConnectionDefinition,
                          loggingConsumer: ConsumerConnection,
                          readDataConsumer: ConsumerConnection,
                          masterConsumer: ConsumerConnection,
                          producer: ProducerConnection,
                          initTopicsAndSizes: TopicsWithSizes,
                          filters: BehaviorSubject[Seq[FilterData]],
                          topicToType: BehaviorSubject[Seq[(String, MessageType)]],
                          avroRegistires: BehaviorSubject[Seq[String]],
                          closed: => Boolean)(implicit uiRenderer: UiRenderer) extends UiComponent {

    type TopicName = String
    private val msgEncoders: String => MessageType = topic => StringMessage
    private val typesRegistry = new TypesRegistry(topicToType, avroRegistires)

    private val refreshTopics: Subject[Unit] = publishSubject()

    private val topicsList: Observable[Seq[TopicInfoRec]] =
        merge(Seq(just(initTopicsAndSizes), refreshTopics.map(_ => masterConsumer.queryTopicsWithSizes())))
                .mapSeq { case (topic, size) => (topic, size, topicToType.map(mappings => mappings.find(_._1 == topic).map(_._2).getOrElse(StringMessage))) }
                .withCachedLatest()

    private val messageTypes: Observable[Seq[MessageType]] =
        avroRegistires.map(avroRegistires => StringMessage +: ZipMessage +: avroRegistires.map(AvroMessage))

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

    private val topicsData = KafkaTopicsMgmt(
        messageTypes = messageTypes,
        onApplyMessageType = applyMessageTypes <<,
        onManageMessageTypes = () => manageMessageTypes(),
        onRequestRefresh = refreshTopics
    )

    override def content(): UiWidget = UiPanel("", Grid(), items = Seq(
        UiTabPanel("grow", tabs = Seq(
            UiTab(label = "Logging", content = new LoggingPane("",
                loggingConsumer = loggingConsumer,
                topicsList = topicsList,
                filters = filters,
                topicsMgmt = topicsData,
                typesRegistry = typesRegistry,
                closed = closed)),
            UiTab(label = "Read Topic", content = new ReadTopicPane("",
                topicsList = topicsList,
                masterCon = masterConsumer,
                readCon = readDataConsumer,
                topicsMgmt = topicsData,
                typesRegistry = typesRegistry,
                closed = closed)),
            UiTab(label = "Produce message", content = new ProduceMessagePane("",
                consumer = masterConsumer,
                producer = producer,
                topicsList = topicsList,
                topicsMgmt = topicsData,
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

    case class ConnectionDefinition(
                                           name: BehaviorSubject[String] = behaviorSubject(""),
                                           kafkaHost: BehaviorSubject[String] = behaviorSubject(""),
                                           topicSettings: BehaviorSubject[Seq[(String, MessageType)]] = behaviorSubject(Seq()),
                                           avroRegistries: BehaviorSubject[Seq[String]] = behaviorSubject(Seq()))

    def isNumber(s: String): Boolean = try {s.toLong; true} catch {case _: Exception => false}

    def search(record: TopicRecord, text: String): Boolean = (record.topic.contains(text)
            || record.key != null && record.key.contains(text)
            || record.value != null && record.value.contains(text))


    def main(args: Array[String]): Unit = {

        val options = new Options()
        options.addRequiredOption("n", "groupname", true, "Group name to connect to kafka server")

        val cli = new DefaultParser().parse(options, args)
        val groupName = cli.getOptionValue("n")

        val appSettings = AppSettings.connect()


        DefaultFxRenderes.runApp(root = new KafkaDataViewerAppPane(
            updateState = KafkaToolUpdate.retrieveUpdateState(),
            connections = appSettings.connections,
            filters = appSettings.filters,
            groupName = groupName
        ))

    }

}
