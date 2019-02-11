package devtools.kafka_data_viewer.ui

import devtools.kafka_data_viewer.KafkaConnTopicsInfo.{KafkaTopicsMgmt, TopicInfoRec}
import devtools.kafka_data_viewer.KafkaDataViewer.isNumber
import devtools.kafka_data_viewer.kafkaconn.Connector._
import devtools.kafka_data_viewer.kafkaconn.MessageFormats.{MessageType, StringMessage}
import devtools.lib.rxext.Observable
import devtools.lib.rxext.Observable.combineLatest
import devtools.lib.rxext.Subject.{behaviorSubject, publishSubject}
import devtools.lib.rxui.UiImplicits._
import devtools.lib.rxui.{UiLabel, _}

import scala.language.postfixOps

class ProduceMessagePane(val layoutData: String,
                         consumer: ConsumerConnection,
                         producer: ProducerConnection,
                         topicsList: Observable[Seq[TopicInfoRec]],
                         topicsMgmt: KafkaTopicsMgmt,
                         msgEncoders: String => MessageType) extends UiObservingComponent {

    sealed trait SelectedTopicType

    case class ExistingTopic(topicName: String) extends SelectedTopicType

    case object CustomTopic extends SelectedTopicType

    private val defaultPartitioner = "Default Partitioner"

    private val topicsFilter = behaviorSubject[String]("")
    private val shownTopics: Observable[Seq[TopicInfoRec]] = Observable.combineLatest(topicsList, topicsFilter)
            .map { case (topics, filter) => topics.filter(_._1.toUpperCase.contains(filter.toUpperCase)) }

    private val selectedTopicData = behaviorSubject[SelectedTopicType](CustomTopic)

    private val topicName = publishSubject[String]()
    (topicName <<< combineLatest(selectedTopicData, topicsList)
            .map {
                case (ExistingTopic(name), _) => name
                case (CustomTopic, _) => ""
            }) ($)

    private val selectedMessageType = behaviorSubject[MessageType]()
    (selectedMessageType <<< selectedTopicData.withLatestFrom(topicsList)
            .map {
                case (ExistingTopic(name), topicsListSeq) =>
                    topicsListSeq.find(_._1 == name).get._3
                case (CustomTopic, _) => Observable.just[MessageType](StringMessage)
            }
            .flatMap(x => x)) ($)

    private val partitionsList = selectedTopicData
            .map { case ExistingTopic(name) => consumer.queryTopicPartitions(name); case _ => Seq() }
            .map(partitions => defaultPartitioner +: partitions.map(_.toString))
            .withCachedLatest()

    private val partition = behaviorSubject[String](defaultPartitioner)
    (partition <<< partitionsList.map(_ => defaultPartitioner)) ($)

    private val key = behaviorSubject("")
    private val message = behaviorSubject("")
    private val sendAction = publishSubject[Unit]()

    for {
        (_, partition, topicName, messageType) <- $(sendAction.withLatestFrom(partition, topicName, selectedMessageType))
        if topicName.trim.nonEmpty
    } {
        val (keyBuf, valueBuf) = DecodingFunction.encode(messageType)(topicName, key.value, message.value)
        val partitioner = partition match {
            case `defaultPartitioner` => DefaultPartitioner
            case x if isNumber(x) => ExactPartition(x.toInt)
            // case _ => throw new Exception("Partition not recognized")
        }
        producer.send(topicName, keyBuf, valueBuf, partitioner)
    }


    override def content(): UiWidget =
        UiPanel(layoutData, Grid("margin 2"), items = Seq(
            UiSplitPane("grow", proportion = 20, orientation = UiHoriz, els = (
                    UiPanel("", Grid("margin 2"), items = Seq(
                        UiLabel(text = "Filter topics by name"),
                        UiText("growx", text = topicsFilter),
                        UiLabel(text = "Double Click to Select Topic or Click Custom Message"),
                        UiButton(text = "Custom Message", onAction = () => selectedTopicData << CustomTopic),
                        new TopicsTablePane("grow",
                            topicsList = shownTopics,
                            topicsMgmt = topicsMgmt,
                            onTopicDblClick = topicName => selectedTopicData << ExistingTopic(topicName)
                        ),
                    )),
                    UiPanel("", Grid(), items = Seq(
                        UiPanel("growx", Grid("cols 2"), items = Seq(
                            UiPanel("growx", Grid("cols 2, margin 2"), items = Seq(
                                UiLabel(text = "Selected Topic:"),
                                UiText("growx", text = topicName, editable = selectedTopicData.map(!_.isInstanceOf[ExistingTopic])),
                                UiLabel(text = "Message Type:"),
                                UiComboT[MessageType](
                                    items = topicsMgmt.messageTypes,
                                    display = _.display,
                                    selection = selectedMessageType,
                                    disabled = selectedTopicData.map(_.isInstanceOf[ExistingTopic])),
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
            ))
        ))
}
