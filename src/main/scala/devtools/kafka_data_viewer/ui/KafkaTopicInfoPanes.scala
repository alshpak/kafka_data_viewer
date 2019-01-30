package devtools.kafka_data_viewer.ui

import devtools.kafka_data_viewer.KafkaConnTopicsInfo.{KafkaConnTopicsData, _}
import devtools.kafka_data_viewer.KafkaDataViewer
import devtools.kafka_data_viewer.KafkaDataViewer.{TopicRecord, isNumber}
import devtools.kafka_data_viewer.kafkaconn.Connector._
import devtools.kafka_data_viewer.kafkaconn.MessageFormats.{AvroMessage, MessageType, StringMessage, ZipMessage}
import devtools.lib.rxext.ListChangeOps.SetList
import devtools.lib.rxext.LoggableOps.{AppendLogOp, LoggingOp, ResetLogOp}
import devtools.lib.rxext.Observable.{combineLatest, empty}
import devtools.lib.rxext.ObservableSeqExt._
import devtools.lib.rxext.Subject.{behaviorSubject, publishSubject}
import devtools.lib.rxext.{BehaviorSubject, Observable, Subject}
import devtools.lib.rxui.UiImplicits._
import devtools.lib.rxui._
import io.reactivex.schedulers.Schedulers

import scala.language.postfixOps

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

    def encode(msgEncoders: String => MessageType)(topic: String, key: String, value: String): (Array[Byte], Array[Byte]) =
        StringMessage.formatter().encode(topic, key) -> msgEncoders(topic).formatter().encode(topic, value)

    def messageTypeDisplay(msgType: MessageType): String = msgType match {
        case StringMessage => "String"
        case ZipMessage => "GZIP"
        case AvroMessage(registry) => s"Avro : $registry"
    }

}

class RecordsOutputTablePane(val layoutData: String,
                             records: Observable[LoggingOp[BinaryTopicRecord]],
                             refreshData: Observable[Unit] = empty(),
                             msgEncoders: String => MessageType,
                             onLoadNewData: Option[Subject[Unit]] = None) extends UiComponent {

    private val builtRecords = publishSubject[LoggingOp[TopicRecord]]()
    private val recordsCache = records.fromLogOps()

    builtRecords <<< records.map(_.map(DecodingFunction.decode(msgEncoders)))

    for ((_, recs) <- refreshData.withLatestFrom(recordsCache)) {
        builtRecords << ResetLogOp()
        builtRecords << AppendLogOp(recs.map(DecodingFunction.decode(msgEncoders)))
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


class TopicsTablePane(val layoutData: String,
                      paneData: KafkaConnTopicsData,
                      selection: Subject[Seq[String]],
                      onTopicDblClick: String => Unit
                     ) extends UiComponent {

    private val selectedItems = behaviorSubject[Seq[TopicInfoRec]](Seq())
    selection <<< selectedItems.mapSeq(x => x._1)

    val menu: Observable[Seq[UiMenuItem]] =
        Observable.merge[Any](Seq(paneData.messageTypes, selectedItems)).withLatestFrom(paneData.messageTypes, selectedItems)
                .map { case (_, types, sel) => Seq[UiMenuItem](
                    UiMenuItem("Refresh topics", onSelect = paneData.onRequestRefresh),
                    UiMenuItem(text = "Change Message Type", subitems =
                            types.map(t => UiMenuItem(text = t.display, onSelect = () => paneData.onApplyMessageType(sel.map(_._1) -> t))) :+
                                    UiMenuItem(text = "Manage Message Types and Settings...", onSelect = paneData.onManageMessageTypes)))
                }

    override def content(): UiWidget = UiTable[TopicInfoRec]("grow",
        items = paneData.topicsList.map(SetList(_)),
        selection = selectedItems,
        onDblClick = Some(rec => onTopicDblClick(rec._1)),
        columns = Seq[UiColumn[TopicInfoRec]](
            UiColumn("topic", v => v._1),
            UiColumn("records", v => v._2.toString),
            UiColumn("type", v => v._3.map(_.display))),
        menu = menu
    )

}

class LoggingPane(val layoutData: String,
                  loggingConsumer: ConsumerConnection,
                  filters: BehaviorSubject[Seq[(String, Seq[String])]],
                  topicsData: KafkaConnTopicsData,
                  typesRegistry: TypesRegistry,
                  closed: => Boolean)(implicit uiRenderer: UiRenderer) extends UiComponent {

    private val consumer = loggingConsumer
    private val topicsWithSizes: Observable[TopicsWithSizes] = topicsData.topicsList.mapSeq(x => (x._1, x._2))

    private val filterTopicsToAdd = behaviorSubject("")

    private val topicsToAdd = publishSubject[Seq[String]]()
    private val addTopics = publishSubject[Unit]()

    private val listeningTopics = behaviorSubject[Seq[String]](Seq())
    private val listeningTopicsInfos = Observable.combineLatest(topicsWithSizes, listeningTopics)
            .map { case (topicInfos, topicNames) => topicInfos.filter(topicInfo => topicNames.contains(topicInfo._1)) }

    private val topicsToRemove = publishSubject[Seq[String]]()
    private val removeTopics = publishSubject[Unit]()

    private val logger: Observable[Seq[BinaryTopicRecord]] = consumer.readTopicsContinually(listeningTopicsInfos.mapSeq(x => x._1).asPublishSubject)
            .subscribeOn(Schedulers.newThread())
            .observeOn(uiRenderer.uiScheduler())
            .filter(_ => !closed)
            .asPublishSubject

    private val shownTopicsToAdd: Observable[TopicsWithSizes] =
        Observable.combineLatest(topicsWithSizes, filterTopicsToAdd, listeningTopics)
                .map(v => v._1
                        .filter(x => x._1.toUpperCase.contains(v._2.toUpperCase))
                        .diff(v._3))

    for (topicsToAdd <- addTopics.withLatestFrom(topicsToAdd).map(_._2)) listeningTopics << (listeningTopics.value ++ topicsToAdd)

    for (topics <- removeTopics.withLatestFrom(topicsToRemove).map(_._2)) listeningTopics << (listeningTopics.value diff topics)

    private val refreshData: Observable[Unit] = topicsData.topicsList.mapSeq(_._3)
            .flatMap((msgTypes: Seq[Observable[MessageType]]) => Observable.merge(msgTypes))
            .map(_ => Unit)

    override def content(): UiWidget = {
        UiPanel(layoutData, Grid("margin 5"), items = Seq(
            UiSplitPane("grow", proportion = 20, orientation = UiHoriz, els = (
                    UiPanel("", Grid(), items = Seq(
                        UiLabel(text = "Add topics to Lister contiually"),
                        new PreFilterPane[String]("growx", currentSelectedItems = listeningTopics,
                            allItems = topicsWithSizes.mapSeq(_._1),
                            applySelectedItems = listeningTopics <<,
                            filters = filters),
                        UiSplitPane("grow", proportion = 50, orientation = UiVert, els = (
                                UiPanel("", Grid(), items = Seq(
                                    UiLabel(text = "Type to filter"),
                                    UiText("growx", text = filterTopicsToAdd),
                                    new TopicsTablePane("grow",
                                        paneData = topicsData,
                                        selection = topicsToAdd,
                                        onTopicDblClick = _ => addTopics << Unit),
                                    UiButton("growx", text = "Add topics to listen", addTopics)
                                )),
                                UiPanel("", Grid(), items = Seq(
                                    UiLabel(text = "Currently listening topics"),
                                    new TopicsTablePane("grow",
                                        paneData = topicsData,
                                        selection = topicsToRemove,
                                        onTopicDblClick = _ => removeTopics << Unit),
                                    UiButton("growx", text = "Remove topic from listening", onAction = removeTopics)
                                ))
                        ))
                    )),
                    new RecordsOutputTablePane("grow", records = logger.map(AppendLogOp(_)), refreshData = refreshData, msgEncoders = typesRegistry.encoder)
            ))
        ))
    }
}

class ReadTopicPane(
                           val layoutData: String = "",
                           consumer: ConsumerConnection,
                           topicsData: KafkaConnTopicsData,
                           typesRegistry: TypesRegistry,
                           closed: => Boolean)(implicit uiRenderer: UiRenderer) extends UiComponent {

    type TopicInfo = (String, Long, MessageType)

    private val sizeToRead = Seq("All", "100", "1k", "10k", "100k")
    private val sizeToReadSelection = behaviorSubject(sizeToRead.head)
    private var sizeToReadCurrent: Long = -1L
    for (sizeStr <- sizeToReadSelection) sizeToReadCurrent = sizeStr.replaceAll("k", "000").trim() match {
        case "All" => -1L
        case "" => -1L
        case x if isNumber(x) => x.toLong
        case _ => 0L
    }
    private val topicsWithSizes = topicsData.topicsList.mapSeq(x => (x._1, x._2))
    private val topicsFilter = behaviorSubject[String]("")
    private val topicWithSizes = combineLatest(topicsWithSizes, topicsFilter)
            .map { case (allTopics, filter) => allTopics.filter(t => t._1.toUpperCase.contains(filter.toUpperCase)) }

    private val selectedTopics = publishSubject[Seq[String]]()
    private val openSelectedTopic = behaviorSubject[String]()
    private val records: Subject[LoggingOp[BinaryTopicRecord]] = publishSubject()
    private val progress: Subject[String] = publishSubject()
    private val onStop: Subject[Unit] = publishSubject()
    private val operationRunning = behaviorSubject(false)
    private val refreshData: Observable[Unit] = topicsData.topicsList.mapSeq((x: TopicInfoRec) => x._3.map(f => (x._1, f)))
            .flatMap((msgTypes: Seq[Observable[(String, MessageType)]]) => Observable.merge(msgTypes))
            .withLatestFrom(openSelectedTopic)
            .filter { case ((topic, format), openedTopic) => topic == openedTopic }
            .map(_ => Unit)

    private var currentRecords: Subject[Seq[BinaryTopicRecord]] = publishSubject()
    for (topic <- openSelectedTopic) {
        records << ResetLogOp()
        operationRunning onNext true
        val recordsSeqs: Observable[Seq[BinaryTopicRecord]] =
            consumer.readTopic(topic, sizeToReadCurrent)(progress, stop = onStop)
        readTrackedRecords(recordsSeqs)
    }
    for (recordsList <- currentRecords) records << AppendLogOp(recordsList)

    private val onNextRead: Subject[Unit] = publishSubject()
    for (_ <- onNextRead) if (currentRecords != null && !operationRunning.value) {
        operationRunning onNext true
        val records = consumer.readNextRecords()(progress, stop = onStop)
        readTrackedRecords(records)
    }

    def readTrackedRecords(records: Observable[Seq[BinaryTopicRecord]]): Unit = {
        val trackedRecords = records.subscribeOn(Schedulers.newThread())
                .doOnNext(_ => if (closed) onStop onNext Unit)
                .observeOn(uiRenderer.uiScheduler())
                .filter(_ => !closed)
                .doOnComplete(() => if (!closed) operationRunning onNext false)
        for (records <- trackedRecords) currentRecords onNext records
    }

    override def content(): UiWidget = {
        UiPanel(layoutData, Grid("margin 5"), items = Seq(
            UiSplitPane("grow", proportion = 20, orientation = UiHoriz, els = (
                    UiPanel("", Grid(), items = Seq(
                        UiLabel(text = "Items to read per partition"),
                        UiCombo("growx", items = sizeToRead, text = sizeToReadSelection),
                        UiLabel(text = "Filter topics by name"),
                        UiText("growx", text = topicsFilter),
                        UiLabel(text = "Read topic by double click"),
                        new TopicsTablePane("grow",
                            paneData = topicsData,
                            selection = publishSubject(),
                            onTopicDblClick = openSelectedTopic <<
                        )
                    )),
                    UiPanel("", Grid(), items = Seq(
                        new RecordsOutputTablePane("grow", records = records, refreshData = refreshData, onLoadNewData = onNextRead, msgEncoders = typesRegistry.encoder)
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

class ProduceMessagePane(val layoutData: String,
                         consumer: ConsumerConnection,
                         producer: ProducerConnection,
                         msgEncoders: String => MessageType,
                         topics: TopicsWithSizes) extends UiComponent {

    private val defaultPartitioner = "Default Partitioner"

    private val topic = behaviorSubject("")
    private val key = behaviorSubject("")
    private val partition = behaviorSubject(defaultPartitioner)
    private val messageType = behaviorSubject("")
    private val message = behaviorSubject("")
    private val sendAction = publishSubject[Unit]()

    private val partitionsList = topic.map(topic =>
        if (topic.isEmpty) Seq(defaultPartitioner)
        else defaultPartitioner +: topics.map(_._1).find(topic ==).map(_ => consumer.queryTopicPartitions(topic).map(_.toString)).getOrElse(Seq()))

    private val messageTypesList = behaviorSubject(Seq(""))

    for ((_, partition) <- sendAction.withLatestFrom(partition); (keyBuf, valueBuf) = DecodingFunction.encode(msgEncoders)(topic.value, key.value, message.value))
        producer.send(topic.value, keyBuf, valueBuf, partition match {
            case `defaultPartitioner` => DefaultPartitioner
            case x if isNumber(x) => ExactPartition(x.toInt)
            case _ => throw new Exception("Partition not recognized")
        })

    override def content(): UiWidget =
        UiPanel(layoutData, Grid("margin 5"), items = Seq(
            UiPanel("growx", Grid("cols 2"), items = Seq(
                UiPanel("grow", Grid("cols 2"), items = Seq(
                    UiLabel(text = "Topic"),
                    UiCombo("growx", text = topic, items = topics.map(_._1)),
                    UiLabel(text = "Key"),
                    UiText("growx", text = key),
                    UiLabel(text = "Partition"),
                    UiCombo(text = partition, items = partitionsList, editable = false),
                    UiLabel(text = "Message Type"),
                    UiCombo(selection = messageType, items = messageTypesList, editable = false)
                )),
                UiButton("growy, valign T", text = "Send", onAction = sendAction)
            )),
            UiText("grow", multi = true, text = message)
        ))
}

