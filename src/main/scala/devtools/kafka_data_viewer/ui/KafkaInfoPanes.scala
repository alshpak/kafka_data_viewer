package devtools.kafka_data_viewer.ui

import devtools.kafka_data_viewer.KafkaDataViewer
import devtools.kafka_data_viewer.KafkaDataViewer.{ConnectionDefinition, isNumber}
import devtools.kafka_data_viewer.kafkaconn.KafkaConnector.{ConsumerConnection, ProducerConnection, TopicRecord, TopicsWithSizes}
import devtools.kafka_data_viewer.kafkaconn.KafkaGroupsInfo
import devtools.kafka_data_viewer.kafkaconn.KafkaGroupsInfo.PartitionAssignmentState
import devtools.lib.rxext.ListChangeOps.SetList
import devtools.lib.rxext.Observable.combineLatest
import devtools.lib.rxext.Subject.{behaviorSubject, publishSubject}
import devtools.lib.rxext.{BehaviorSubject, Observable, Subject}
import devtools.lib.rxui.UiImplicits._
import devtools.lib.rxui._
import io.reactivex.schedulers.Schedulers

import scala.language.postfixOps

class RecordsOutputTablePane(val layoutData: String,
                             records: Observable[_ <: Observable[Seq[TopicRecord]]],
                             onRefreshData: Option[Subject[Unit]] = None) extends UiComponent {

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
        records = records,
        compare = KafkaDataViewer.search,
        onRefreshData = onRefreshData,
        fields = fieldDefs.map(fd => fd.title -> fd.value),
        valueField = r => r.value,
        sorting = Function.unlift(title => fieldDefs.find(_.title == title).flatMap(_.sorting))
    )
}

class LoggingPane(val layoutData: String,
                  loggingConsumer: ConsumerConnection,
                  filters: BehaviorSubject[Seq[(String, Seq[String])]],
                  initTopicsAndSizes: TopicsWithSizes,
                  closed: => Boolean)(implicit uiRenderer: UiRenderer) extends UiComponent {
    private val consumer = loggingConsumer
    private val topics: Observable[Seq[String]] = Observable.just(initTopicsAndSizes.map(_._1))

    private val filterTopicsToAdd = behaviorSubject("")

    private val topicsToAdd = publishSubject[Seq[String]]()
    private val addTopics = publishSubject[Any]()

    private val listeningTopics = behaviorSubject[Seq[String]](Seq())

    private val topicsToRemove = publishSubject[Seq[String]]()
    private val removeTopics = publishSubject[Any]()

    private val logger: Observable[Seq[TopicRecord]] = consumer.readTopicsContinually(listeningTopics)
            .subscribeOn(Schedulers.newThread())
            .observeOn(uiRenderer.uiScheduler())
            .filter(_ => !closed)

    private val shownTopicsToAdd: Observable[Seq[String]] =
        Observable.combineLatest(topics, filterTopicsToAdd, listeningTopics)
                .map(v => v._1
                        .filter(x => x.toUpperCase.contains(v._2.toUpperCase))
                        .diff(v._3))

    for (topics <- addTopics.withLatestFrom(topicsToAdd).map(_._2))
        listeningTopics onNext (listeningTopics.value ++ topics)

    for (topics <- removeTopics.withLatestFrom(topicsToRemove).map(_._2))
        listeningTopics onNext (listeningTopics.value diff topics)

    override def content(): UiWidget = {
        UiPanel(layoutData, Grid("margin 5"), items = Seq(
            UiSplitPane("grow", proportion = 20, orientation = UiHoriz, els = (
                    UiPanel("", Grid(), items = Seq(
                        UiLabel(text = "Add topics to Lister contiually"),
                        new PreFilterPane[String]("growx", currentSelectedItems = listeningTopics,
                            allItems = topics,
                            applySelectedItems = items => listeningTopics onNext items,
                            filters = filters),
                        UiSplitPane("grow", proportion = 50, orientation = UiVert, els = (
                                UiPanel("", Grid(), items = Seq(
                                    UiLabel(text = "Type to filter"),
                                    UiText("growx", text = filterTopicsToAdd),
                                    UiList[String]("grow", items = shownTopicsToAdd, selection = topicsToAdd, onDblClick = addTopics, multi = true),
                                    UiButton("growx", text = "Add topics to listen", addTopics)
                                )),
                                UiPanel("", Grid(), items = Seq(
                                    UiLabel(text = "Currently listening topics"),
                                    UiList[String]("grow", items = listeningTopics, selection = topicsToRemove, multi = true, onDblClick = removeTopics),
                                    UiButton("growx", text = "Remove topic from listening", onAction = removeTopics)
                                ))
                        ))
                    )),
                    new RecordsOutputTablePane("grow", records = Observable.just(logger))
            ))
        ))
    }
}

class ReadTopicPane(
                           val layoutData: String = "",
                           consumer: ConsumerConnection,
                           initTopicsAndSizes: TopicsWithSizes,
                           closed: => Boolean)(implicit uiRenderer: UiRenderer) extends UiComponent {

    type TopicInfo = (String, Long)

    private val sizeToRead = Seq("All", "100", "1k", "10k", "100k")
    private val sizeToReadSelection = behaviorSubject(sizeToRead.head)
    private var sizeToReadCurrent: Long = -1L
    for (sizeStr <- sizeToReadSelection) sizeToReadCurrent = sizeStr.replaceAll("k", "000").trim() match {
        case "All" => -1L
        case "" => -1L
        case x if isNumber(x) => x.toLong
        case _ => 0L
    }
    private val refreshTopics = behaviorSubject[Any](Unit)
    private val allTopicsWithSizes = behaviorSubject(initTopicsAndSizes)
    allTopicsWithSizes <<< refreshTopics.map(_ => consumer.queryTopicsWithSizes())
    private val topicsFilter = behaviorSubject[String]("")
    private val topicWithSizes = combineLatest(allTopicsWithSizes, topicsFilter)
            .map { case (allTopics, filter) => allTopics.filter(t => t._1.toUpperCase.contains(filter.toUpperCase)) }
            .map(SetList(_))

    private val selectedTopic: Subject[TopicInfo] = publishSubject()
    private val records: Subject[Subject[Seq[TopicRecord]]] = publishSubject()
    private val progress: Subject[String] = publishSubject()
    private val onStop: Subject[Unit] = publishSubject()
    private val operationRunning = behaviorSubject(false)


    private var currentRecords: Subject[Seq[TopicRecord]] = _
    for (topic <- selectedTopic) {
        currentRecords = publishSubject()
        records onNext currentRecords
        operationRunning onNext true
        val recordsSeqs: Observable[Seq[TopicRecord]] = consumer.readTopic(topic._1, sizeToReadCurrent)(progress, stop = onStop)
        readTrackedRecords(recordsSeqs)
    }

    private val onNextRead: Subject[Unit] = publishSubject()
    for (_ <- onNextRead) if (currentRecords != null && !operationRunning.value) {
        operationRunning onNext true
        val records = consumer.readNextRecords()(progress, stop = onStop)
        readTrackedRecords(records)
    }

    def readTrackedRecords(records: Observable[Seq[TopicRecord]]): Unit = {
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
                        UiTable[TopicInfo]("grow",
                            items = topicWithSizes,
                            onDblClick = selectedTopic,
                            disabled = operationRunning,
                            columns = Seq(
                                UiColumn("topic", v => v._1),
                                UiColumn("records", v => v._2.toString))
                        ),
                        UiButton("growx", text = "Refresh topics", onAction = refreshTopics)
                    )),
                    UiPanel("", Grid(), items = Seq(
                        new RecordsOutputTablePane("grow", records = records, onRefreshData = onNextRead)
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
                         producer: ProducerConnection,
                         topics: TopicsWithSizes) extends UiComponent {
    private val topic = behaviorSubject("")
    private val key = behaviorSubject("")
    private val message = behaviorSubject("")
    private val sendAction = publishSubject[Any]()

    for (_ <- sendAction) producer.send(topic.value, key.value, message.value)

    override def content(): UiWidget =
        UiPanel(layoutData, Grid("margin 5"), items = Seq(
            UiPanel("growx", Grid("cols 3"), items = Seq(
                UiLabel(text = "Topic"),
                UiCombo("growx", text = topic, items = topics.map(_._1)),
                UiButton(text = "Send", onAction = sendAction),
                UiLabel(text = "Key"),
                UiText("growx", text = key)
            )),
            UiText("grow", multi = true, text = message)
        ))
}

class ConsumersInfoPane(val layoutData: String = "", condef: ConnectionDefinition) extends UiComponent {

    trait Elem {
        val name: String = ""
        val partition: String = ""
        val currentOffset = ""
        val logEndOffset = ""
        val lag = ""
        val consumerId = ""
        val host = ""
        val clientId = ""

        def query(): Seq[Elem] = Seq()

        private lazy val items = behaviorSubject[Seq[Elem]](query())

        def subitems(): Observable[Seq[Elem]] = items

        def refresh(): Unit = items onNext query()
    }

    case class Root(override val name: String, queryGroups: () => Seq[Elem]) extends Elem {
        override def query(): Seq[Elem] = queryGroups()
    }

    case class Group(override val name: String, queryPartitions: String => Seq[Elem]) extends Elem {
        override def query(): Seq[Elem] = queryPartitions(name)
    }

    case class PartitionInfo(state: PartitionAssignmentState) extends Elem {
        override val name: String = state.topic.get
        override val partition: String = state.partition.map(_.toString).getOrElse("-")
        override val currentOffset: String = state.offset.map(_.toString).getOrElse("-")
        override val logEndOffset: String = state.logEndOffset.map(_.toString).getOrElse("-")
        override val lag: String = state.lag.map(_.toString).getOrElse("-")
        override val consumerId: String = state.consumerId.getOrElse("-")
        override val host: String = state.host.getOrElse("-")
        override val clientId: String = state.clientId.getOrElse("-")
    }

    private val items = Seq(
        Some(
            Root("KafkaGroups",
                queryGroups = () => KafkaGroupsInfo.getKafkaGroups(condef.kafkaHost).map(
                    Group(_, name => KafkaGroupsInfo.describeKafkaGroup(condef.kafkaHost, name)
                            .map(state => PartitionInfo(state)).sortBy(x => (x.name, x.partition))))
                        .sortBy(_.name),
            )),
        Option(condef.zoo).filter(!_.isEmpty).map(zoo =>
            Root("ZooGroups",
                queryGroups = () => KafkaGroupsInfo.getZooGroups(zoo).map(
                    Group(_, name => KafkaGroupsInfo.describeZooGroup(zoo, name)
                            .map(state => PartitionInfo(state)).sortBy(x => (x.name, x.partition)))).sortBy(_.name),
            )))
            .filter(_.isDefined).map(_.get)

    override def content(): UiWidget = UiPanel(layoutData, Grid(), items = Seq(
        UiTree[Elem]("grow", items = items,
            columns = Seq(
                UiColumn("Group", _.name),
                UiColumn("Partition", _.partition),
                UiColumn("Current Offset", _.currentOffset),
                UiColumn("Log End Offset", _.logEndOffset),
                UiColumn("Lag", _.lag),
                UiColumn("Consumer ID", _.consumerId),
                UiColumn("Host", _.host),
                UiColumn("Client ID", _.clientId)
            ),
            subitems = _.subitems(),
            expanded = _ => false,
            hasChildren = {
                case Root(_, _) => true
                case Group(_, _) => true
                case _ => false
            },
            menu = Some {
                case x: Root => Seq(UiMenuItem("Refresh", () => x.refresh()))
                case x: Group => Seq(UiMenuItem("Refresh", () => x.refresh()))
                case _ => Seq()
            }
        )
    ))
}
