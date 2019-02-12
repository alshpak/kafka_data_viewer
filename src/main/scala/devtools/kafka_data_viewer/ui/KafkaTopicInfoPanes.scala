package devtools.kafka_data_viewer.ui

import scala.language.postfixOps

import io.reactivex.schedulers.Schedulers

import devtools.kafka_data_viewer.KafkaConnTopicsInfo.{KafkaTopicsMgmt, _}
import devtools.kafka_data_viewer.KafkaDataViewer
import devtools.kafka_data_viewer.KafkaDataViewer.{FilterData, TopicRecord, isNumber}
import devtools.kafka_data_viewer.kafkaconn.Connector._
import devtools.kafka_data_viewer.kafkaconn.MessageFormats.{AvroMessage, MessageType, StringMessage, ZipMessage}
import devtools.lib.rxext.ListChangeOps.SetList
import devtools.lib.rxext.LoggableOps.{AppendLogOp, LoggingOp, ResetLogOp}
import devtools.lib.rxext.Observable.{combineLatest, empty, just}
import devtools.lib.rxext.ObservableSeqExt._
import devtools.lib.rxext.Subject.{behaviorSubject, publishSubject}
import devtools.lib.rxext.{BehaviorSubject, Observable, Subject}
import devtools.lib.rxui.UiImplicits._
import devtools.lib.rxui._

/*
class TypesRegistry(
                           private val topicToType: BehaviorSubject[Seq[(String, MessageType)]],
                           private val avroRegistires: BehaviorSubject[Seq[String]]) {

    private var topicToTypeCache = Map[String, MessageType]()
    for (items <- topicToType) topicToTypeCache = items.toMap

    def listRegistries(): Seq[String] = avroRegistires.value

    def addRegistry(registry: String): Unit = avroRegistires << avroRegistires.value :+ registry

    def encoder(topic: String): MessageType = topicToTypeCache.getOrElse(topic, StringMessage)

    def changeType(topic: String, messageType: MessageType): Unit = {
        topicToType << topicToType.value.filterNot(_._1 == topic) :+ (topic -> messageType)
    }
}
*/

class TopicsInfoList(val topicsWithSizes: Observable[TopicsWithSizes], onRefresh: => Unit) {
    def requestRefresh(): Unit = onRefresh
}

object DecodingFunction {

    def decode(msgEncoders: String => MessageType)(rec: BinaryTopicRecord): TopicRecord =
        TopicRecord(
            topic = rec.topic,
            partition = rec.partition,
            offset = rec.offset,
            time = rec.time,
            key = StringMessage.formatter().decode(rec.topic, rec.key),
            value = msgEncoders(rec.topic).formatter().decode(rec.topic, rec.value))

    def encode(valueType: MessageType)(topic: String, key: String, value: String): (Array[Byte], Array[Byte]) =
        StringMessage.formatter().encode(topic, key) -> valueType.formatter().encode(topic, value)

    def messageTypeDisplay(msgType: MessageType): String = msgType match {
        case StringMessage => "String"
        case ZipMessage => "GZIP"
        case AvroMessage(registry) => s"Avro : $registry"
    }

}

class RecordsOutputTablePane(val layoutData: String,
                             records: Observable[LoggingOp[BinaryTopicRecord]],
                             refreshData: Observable[Unit] = empty(),
                             msgTypes: Observable[(String, MessageType)],
                             onLoadNewData: Option[Subject[Unit]] = None) extends UiObservingComponent {

    private val builtRecords = publishSubject[LoggingOp[TopicRecord]]()
    private val recordsCache = records.fromLogOps()

    private val encFuncCache = scala.collection.mutable.HashMap[String, MessageType]()
    for (msgType <- $(msgTypes)) encFuncCache += msgType._1 -> msgType._2
    private val encFunc: String => MessageType = encFuncCache

    (builtRecords <<< records.map(recsSeq => recsSeq.map(DecodingFunction.decode(encFunc)))) ($)

    for ((_, recsSeq) <- $(refreshData.withLatestFrom(recordsCache))) {
        builtRecords << ResetLogOp()
        builtRecords << AppendLogOp(recsSeq.map(DecodingFunction.decode(encFunc)))
    }

    case class FieldDef(title: String, value: TopicRecord => String, sorting: Option[(TopicRecord, TopicRecord) => Boolean])

    private val fieldDefs = Seq(
        FieldDef("Topic", _.topic, Some((t1, t2) => t1.topic.compareTo(t2.topic) < 0)),
        FieldDef("Created", _.time.toString, Some((t1, t2) => t1.time.compareTo(t2.time) < 0)),
        FieldDef("Partition", _.partition.toString, Some((t1, t2) => t1.partition < t2.partition)),
        FieldDef("Offset", _.offset.toString, Some((t1, t2) => t1.offset < t2.offset)),
        FieldDef("Key", _.key, Some((t1, t2) => t1.key.compareTo(t2.key) < 0)),
        FieldDef("Value", _.value.split("\n").map(_.trim).mkString(" "), None)
    )

    override def content(): UiWidget = new TableOutputDataPane[TopicRecord](layoutData,
        records = builtRecords,
        compare = KafkaDataViewer.search,
        onLoadNewData = onLoadNewData,
        fields = fieldDefs.map(fd => fd.title -> fd.value),
        valueField = r => Option(r).map(_.value).getOrElse(""),
        sorting = Function.unlift(title => fieldDefs.find(_.title == title).flatMap(_.sorting))
    )
}

class TopicsOverviewPane(val layoutData: String = "")

class TopicsTablePane(val layoutData: String,
                      topicsList: Observable[Seq[TopicInfoRec]],
                      topicsMgmt: KafkaTopicsMgmt,
                      selection: Subject[Seq[String]] = publishSubject(),
                      onTopicDblClick: String => Unit,
                      disabled: Observable[Boolean] = just(false)
                     ) extends UiObservingComponent {

    private val selectedItems = behaviorSubject[Seq[TopicInfoRec]](Seq())
    (selection <<< selectedItems.mapSeq((x: TopicInfoRec) => x._1)) ($)

    val menu: Observable[Seq[UiMenuItem]] =
        Observable.merge[Any](Seq(topicsMgmt.messageTypes, selectedItems)).withLatestFrom(topicsMgmt.messageTypes, selectedItems)
                .map { case (_, types, sel) => Seq[UiMenuItem](
                    UiMenuItem("Refresh topics", onSelect = topicsMgmt.onRequestRefresh),
                    UiMenuItem(text = "Change Message Type", subitems =
                            types.map(t => UiMenuItem(text = t.display, onSelect = () => topicsMgmt.onApplyMessageType(sel.map(_._1) -> t))) :+
                                    UiMenuItem(text = "Manage Message Types and Settings...", onSelect = topicsMgmt.onManageMessageTypes)))
                }

    override def content(): UiWidget = UiTable[TopicInfoRec]("grow",
        items = topicsList.map(SetList(_)),
        selection = selectedItems,
        onDblClick = Some(rec => onTopicDblClick(rec._1)),
        columns = Seq[UiColumn[TopicInfoRec]](
            UiColumn("topic", v => v._1),
            UiColumn("records", v => v._2.toString),
            UiColumn("type", v => v._3.map(_.display))),
        menu = menu,
        disabled = disabled
    )

}

class LoggingPane(val layoutData: String,
                  loggingConsumer: ConsumerConnection,
                  filters: BehaviorSubject[Seq[FilterData]],
                  topicsList: Observable[Seq[TopicInfoRec]],
                  topicsMgmt: KafkaTopicsMgmt,
                  closed: => Boolean)(implicit uiRenderer: UiRenderer) extends UiObservingComponent {

    private val consumer = loggingConsumer

    val msgTypes: Observable[(String, MessageType)] = topicsList.mapSeq(topicRec => topicRec._3.map(topicRec._1 -> _)).flatMap(Observable.merge(_))

    //private val msgEncoders: Observable[String => MessageType] = topicsList.mapSeq(topicRec => topicRec._3.map(topicRec._1 -> _))

    private val filterTopicsToAdd = behaviorSubject("")

    private val topicsToAdd = publishSubject[Seq[String]]()
    private val addTopics = publishSubject[Unit]()

    private val listeningTopics = behaviorSubject[Seq[String]](Seq())
    private val listeningTopicsInfo: Observable[Seq[TopicInfoRec]] = Observable.combineLatest(topicsList, listeningTopics)
            .map { case (topics, topicNames) => topics.filter(topicInfo => topicNames.contains(topicInfo._1)) }

    private val logger: Observable[Seq[BinaryTopicRecord]] = consumer.readTopicsContinually(listeningTopics)
            .subscribeOn(Schedulers.newThread())
            .observeOn(uiRenderer.uiScheduler())
            .filter(_ => !closed)
            .asPublishSubject($)

    private val shownTopicsToAdd: Observable[Seq[TopicInfoRec]] =
        Observable.combineLatest(topicsList, filterTopicsToAdd, listeningTopics)
                .map { case (topics, filter, listeningNames) =>
                    topics.filter(_._1.toUpperCase().contains(filter.toUpperCase()))
                            .filterNot(topicInfo => listeningNames.contains(topicInfo._1))
                }

    private val topicsToRemove = publishSubject[Seq[String]]()
    private val removeTopics = publishSubject[Unit]()

    for (topicsToAdd <- $(addTopics.withLatestFrom(topicsToAdd).map(_._2))) listeningTopics << (listeningTopics.value ++ topicsToAdd)

    for (topics <- $(removeTopics.withLatestFrom(topicsToRemove).map(_._2))) listeningTopics << (listeningTopics.value diff topics)

    private val refreshData: Observable[Unit] = topicsList.mapSeq(_._3)
            .flatMap((msgTypes: Seq[Observable[MessageType]]) => Observable.merge(msgTypes))
            .map(_ => Unit)

    override def content(): UiWidget = {
        UiPanel(layoutData, Grid("margin 2"), items = Seq(
            UiSplitPane("grow", proportion = 20, orientation = UiHoriz, els = (
                    UiPanel("", Grid("margin 2"), items = Seq(
                        UiLabel(text = "Add topics to Lister continually"),
                        new PreFilterPane[String]("growx", currentSelectedItems = listeningTopics,
                            allItems = topicsList.mapSeq(_._1),
                            applySelectedItems = listeningTopics <<,
                            filters = filters),
                        UiSplitPane("grow", proportion = 50, orientation = UiVert, els = (
                                UiPanel("", Grid("margin 2"), items = Seq(
                                    UiLabel(text = "Type to filter"),
                                    UiText("growx", text = filterTopicsToAdd),
                                    new TopicsTablePane("grow",
                                        topicsList = shownTopicsToAdd,
                                        topicsMgmt = topicsMgmt,
                                        selection = topicsToAdd,
                                        onTopicDblClick = _ => addTopics << Unit),
                                    UiButton("growx", text = "Add topics to listen", addTopics)
                                )),
                                UiPanel("", Grid("margin 2"), items = Seq(
                                    UiLabel(text = "Currently listening topics"),
                                    new TopicsTablePane("grow",
                                        topicsList = listeningTopicsInfo,
                                        topicsMgmt = topicsMgmt,
                                        selection = topicsToRemove,
                                        onTopicDblClick = _ => removeTopics << Unit),
                                    UiButton("growx", text = "Remove topic from listening", onAction = removeTopics)
                                ))
                        ))
                    )),
                    new RecordsOutputTablePane("grow", records = logger.map(AppendLogOp(_)), refreshData = refreshData, msgTypes = msgTypes)
            ))
        ))
    }
}

class ReadTopicPane(
                           val layoutData: String = "",
                           masterCon: ConsumerConnection,
                           readCon: ConsumerConnection,
                           topicsList: Observable[Seq[TopicInfoRec]],
                           topicsMgmt: KafkaTopicsMgmt,
                           closed: => Boolean)(implicit uiRenderer: UiRenderer) extends UiObservingComponent {

    type TopicInfo = (String, Long, MessageType)
    type TopicToSize = (String, Long)

    val msgTypes: Observable[(String, MessageType)] = topicsList.mapSeq(topicRec => topicRec._3.map(topicRec._1 -> _)).flatMap(Observable.merge(_))

    private val sizeToRead = Seq("All", "100", "1k", "10k", "100k")
    private val sizeToReadSelection = behaviorSubject(sizeToRead.head)
    private var sizeToReadCurrent: Long = -1L
    for (sizeStr <- $(sizeToReadSelection)) sizeToReadCurrent = sizeStr.replaceAll("k", "000").trim() match {
        case "All" => -1L
        case "" => -1L
        case x if isNumber(x) => x.toLong
        case _ => 0L
    }
    private val topicsFilter = behaviorSubject[String]("")
    private val shownTopics: Observable[Seq[TopicInfoRec]] = Observable.combineLatest(topicsList, topicsFilter)
            .map { case (topics, filter) => topics.filter(_._1.toUpperCase.contains(filter.toUpperCase)) }

    private val openSelectedTopic = behaviorSubject[String]()
    private val records: Subject[LoggingOp[BinaryTopicRecord]] = publishSubject()
    private val progress: Subject[String] = publishSubject()
    private val onStop: Subject[Unit] = publishSubject()
    private val operationRunning = behaviorSubject(false)
    private val refreshData: Observable[Unit] = topicsList.mapSeq((x: TopicInfoRec) => x._3.map(f => (x._1, f)))
            .flatMap((msgTypes: Seq[Observable[(String, MessageType)]]) => Observable.merge(msgTypes))
            .withLatestFrom(openSelectedTopic)
            .filter { case ((topic, format), openedTopic) => topic == openedTopic }
            .map(_ => Unit)

    private var currentRecords: Subject[Seq[BinaryTopicRecord]] = publishSubject()
    for (topic <- $(openSelectedTopic)) {
        records << ResetLogOp()
        operationRunning onNext true
        val recordsSeqs: Observable[Seq[BinaryTopicRecord]] =
            readCon.readTopic(topic, sizeToReadCurrent)(progress, stop = onStop)
        readTrackedRecords(recordsSeqs)
    }
    for (recordsList <- $(currentRecords)) records << AppendLogOp(recordsList)

    private val onNextRead: Subject[Unit] = publishSubject()
    for (_ <- $(onNextRead)) if (currentRecords != null && !operationRunning.value) {
        operationRunning onNext true
        val records = readCon.readNextRecords()(progress, stop = onStop)
        readTrackedRecords(records)
    }

    def readTrackedRecords(records: Observable[Seq[BinaryTopicRecord]]): Unit = {
        val trackedRecords = records.subscribeOn(Schedulers.newThread())
                .doOnNext(_ => if (closed) onStop onNext Unit)
                .observeOn(uiRenderer.uiScheduler())
                .filter(_ => !closed)
                .doOnComplete(() => if (!closed) operationRunning onNext false)
        for (records <- $(trackedRecords)) currentRecords << records
    }

    override def content(): UiWidget = {
        UiPanel(layoutData, Grid("margin 2"), items = Seq(
            UiSplitPane("grow", proportion = 20, orientation = UiHoriz, els = (
                    UiPanel("", Grid("margin 2"), items = Seq(
                        UiLabel(text = "Items to read per partition"),
                        UiCombo("growx", items = sizeToRead, text = sizeToReadSelection),
                        UiLabel(text = "Filter topics by name"),
                        UiText("growx", text = topicsFilter),
                        UiLabel(text = "Read topic by double click"),
                        new TopicsTablePane("grow",
                            topicsList = shownTopics,
                            topicsMgmt = topicsMgmt,
                            onTopicDblClick = openSelectedTopic <<,
                            disabled = operationRunning
                        )
                    )),
                    UiPanel("", Grid(), items = Seq(
                        new RecordsOutputTablePane("grow", records = records, refreshData = refreshData, onLoadNewData = onNextRead, msgTypes = msgTypes)
                    ))
            )),
            UiPanel("growx", Grid("cols 2"), items = Seq(
                UiLabel(text = "Progress"),
                UiLink("growx", text = combineLatest(progress, operationRunning).observeOn(uiRenderer.uiScheduler())
                        .map(p => p._1 + (if (p._2) " <A>Stop Reading</A>" else "")).filter(_ => !closed),
                    onAction = onStop)
            ))
        ))
    }
}

