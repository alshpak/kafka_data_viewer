package devtools.kafka_data_viewer.ui

import scala.language.postfixOps

import devtools.kafka_data_viewer.KafkaConnTopicsInfo.{KafkaTopicsMgmt, TopicInfoRec}
import devtools.kafka_data_viewer.KafkaDataViewer.{ProducerSettings, isNumber}
import devtools.kafka_data_viewer.kafkaconn.Connector._
import devtools.kafka_data_viewer.kafkaconn.MessageFormats.{MessageType, StringMessage}
import devtools.kafka_data_viewer.ui.ProducerPane._
import devtools.lib.rxext.ListChangeOps.{AddItems, ListChangeOp, SetList}
import devtools.lib.rxext.Observable.{combineLatest, just}
import devtools.lib.rxext.Subject.{behaviorSubject, publishSubject}
import devtools.lib.rxext.{BehaviorSubject, Observable}
import devtools.lib.rxui.UiImplicits._
import devtools.lib.rxui.{UiLabel, _}

object ProducerPane {

    sealed trait TopicType

    case class ExistingTopic(topicName: String) extends TopicType

    case object CustomTopic extends TopicType

}

class ProducerPane(val layoutData: String,
                   consumer: ConsumerConnection,
                   producer: ProducerConnection,
                   settings: BehaviorSubject[Seq[ProducerSettings]],
                   topicsList: Observable[Seq[TopicInfoRec]],
                   topicsMgmt: KafkaTopicsMgmt,
                   msgEncoders: String => MessageType) extends UiObservingComponent {


    private val topicsFilter = behaviorSubject[String]("")
    private val shownTopics: Observable[Seq[TopicInfoRec]] = combineLatest(topicsList, topicsFilter)
            .map { case (topics, filter) => topics.filter(_._1.toUpperCase.contains(filter.toUpperCase)) }

    private val openTopicProducer = publishSubject[TopicType]()

    case class ProducerEntry(
                                    topicData: TopicType,
                                    settings: ProducerSettings) {
        def label: String = topicData match {
            case ExistingTopic(topicName) => topicName
            case CustomTopic => "Custom Message"
        }
    }

    private val producerEntriesOps = behaviorSubject[ListChangeOp[ProducerEntry]](SetList[ProducerEntry](
        settings.value.map(producerData => ProducerEntry(
            topicData = if (producerData.custom.value) CustomTopic else ExistingTopic(producerData.topic.value),
            settings = producerData
        )).sortBy(_.settings.order.value)
    ))
    private val producerSelection = publishSubject[ProducerEntry]()
    for (topicData <- $(openTopicProducer)) {
        val producerData = ProducerSettings()
        settings << settings.value :+ producerData
        val entry = ProducerEntry(topicData = topicData, settings = producerData)
        producerEntriesOps << AddItems(Seq(entry))
        producerSelection << entry
    }
    private val producerClosed = publishSubject[ProducerEntry]()
    for (topicData <- $(producerClosed))
        settings << settings.value.filterNot(topicData.settings ==)

    private val tabsOrdered = publishSubject[Seq[(ProducerEntry, Int)]]()
    for (ordersSeq <- $(tabsOrdered); (entry, order) <- ordersSeq)
        entry.settings.order << order

    override def content(): UiWidget =
        UiPanel(layoutData, Grid("margin 2"), items = Seq(
            UiSplitPane("grow", proportion = 20, orientation = UiHoriz, els = (
                    UiPanel("", Grid("margin 2"), items = Seq(
                        UiLabel(text = "Filter topics by name"),
                        UiText("growx", text = topicsFilter),
                        UiLabel(text = "Double Click to Select Topic or Click Custom Message"),
                        UiButton(text = "Custom Message", onAction = () => openTopicProducer << CustomTopic),
                        new TopicsTablePane("grow",
                            topicsList = shownTopics,
                            topicsMgmt = topicsMgmt,
                            onTopicDblClick = topicName => openTopicProducer << ExistingTopic(topicName)
                        ),
                    )),
                    UiTabPanelExt[ProducerEntry](
                        tabs = producerEntriesOps,
                        tab = entry => UiTab(
                            label = entry.label,
                            content = new ProduceMessagePane(
                                topicData = entry.topicData,
                                settings = entry.settings,
                                topicsList = topicsList,
                                consumer = consumer,
                                producer = producer,
                                topicsMgmt = topicsMgmt)
                        ),
                        selection = producerSelection,
                        closeable = true,
                        onClose = producerClosed <<,
                        onTabsOrdered = tabsOrdered <<
                    )

            ))
        ))
}

class ProduceMessagePane(val layoutData: String = "",
                         topicData: TopicType,
                         topicsList: Observable[Seq[TopicInfoRec]],
                         consumer: ConsumerConnection,
                         topicsMgmt: KafkaTopicsMgmt,
                         producer: ProducerConnection,
                         settings: ProducerSettings,
                        ) extends UiObservingComponent {

    private val defaultPartitioner = "Default Partitioner"

    private val topicName = behaviorSubject[String](topicData match {
        case ExistingTopic(name) => name
        case CustomTopic => settings.topic.value
    })

    private val selectedMessageType = behaviorSubject[MessageType]()
    (selectedMessageType <<< combineLatest(just(topicData), topicsList)
            .map {
                case (ExistingTopic(name), topicsListSeq) => topicsListSeq.find(_._1 == name).map(_._3).getOrElse(just(settings.msgType.value))
                case (CustomTopic, _) => just(settings.msgType.value)
            }
            .flatMap(x => x)) ($)

    private val partitionsList = combineLatest(just(topicData), topicsList)
            .map {
                case (ExistingTopic(name), topicsListSeq) => topicsListSeq.find(_._1 == name).map(_ => consumer.queryTopicPartitions(name)).getOrElse(Seq())
                case _ => Seq()
            }
            .map(partitions => defaultPartitioner +: partitions.map(_.toString))
            .withCachedLatest()

    private val partition = behaviorSubject[String](defaultPartitioner)
    (partition <<< partitionsList.map(_ => defaultPartitioner)) ($)

    settings.custom << (topicData == CustomTopic)
    (settings.topic <<< topicName) ($)
    (settings.msgType <<< selectedMessageType.value) ($)
    (settings.partition <<< partition.map(partitionName => if (partitionName == defaultPartitioner) None else partitionName.toInt)) ($)

    private val key = settings.key
    private val message = settings.value
    private val sendAction = publishSubject[Unit]()

    for {
        (_, partition, topicName, messageType) <- $(sendAction.withLatestFrom(partition, topicName, selectedMessageType))
        if topicName.trim.nonEmpty
    } {
        val (keyBuf, valueBuf) = DecodingFunction.encode(messageType)(topicName, key.value, message.value)
        val partitioner = partition match {
            case `defaultPartitioner` => DefaultPartitioner
            case x if isNumber(x) => ExactPartition(x.toInt)
        }
        producer.send(topicName, keyBuf, valueBuf, partitioner)
    }

    override def content(): UiWidget = UiPanel("", Grid(), items = Seq(
        UiPanel("growx", Grid("cols 2"), items = Seq(
            UiPanel("growx", Grid("cols 2, margin 2"), items = Seq(
                UiLabel(text = "Selected Topic:"),
                UiText("growx", text = topicName, editable = topicData == CustomTopic),
                UiLabel(text = "Message Type:"),
                UiComboT[MessageType](
                    items = topicsMgmt.messageTypes,
                    display = _.display,
                    selection = selectedMessageType,
                    disabled = topicData.isInstanceOf[ExistingTopic]),
                UiLabel(text = "Partition:"),
                UiCombo(selection = partition, items = partitionsList, editable = false),
                UiLabel(text = "Key:"),
                UiText("growx", text = key),
            )),
            UiButton("growy", text = "Send", onAction = sendAction),
        )),
        UiPanel("grow", Grid("margin 2"), items = Seq(
            UiLabel(text = "Message:"),
            UiText("grow", multi = true, text = message)
        ))
    ))
}