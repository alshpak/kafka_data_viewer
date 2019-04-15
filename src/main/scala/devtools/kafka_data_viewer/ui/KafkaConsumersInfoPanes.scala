package devtools.kafka_data_viewer.ui

import devtools.kafka_data_viewer.KafkaDataViewer.ConnectionDefinition
import devtools.kafka_data_viewer.kafkaconn.KafkaGroupsInfo
import devtools.kafka_data_viewer.kafkaconn.KafkaGroupsInfo.PartitionAssignmentState
import devtools.lib.rxext.Subject.behaviorSubject
import devtools.lib.rxext.{Observable, Subject}
import devtools.lib.rxui.UiImplicits._
import devtools.lib.rxui._

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
                queryGroups = () => KafkaGroupsInfo.getKafkaGroups(condef.kafkaHost.value).map(
                    Group(_, name => KafkaGroupsInfo.describeKafkaGroup(condef.kafkaHost.value, name)
                            .map(state => PartitionInfo(state)).sortBy(x => (x.name, x.partition))))
                        .sortBy(_.name),
            ))
    ) /*,
        Option(condef.zoo).filter(!_.isEmpty).map(zoo =>
            Root("ZooGroups",
                queryGroups = () => KafkaGroupsInfo.getZooGroups(zoo).map(
                    Group(_, name => KafkaGroupsInfo.describeZooGroup(zoo, name)
                            .map(state => PartitionInfo(state)).sortBy(x => (x.name, x.partition)))).sortBy(_.name),
            )))*/
            .filter(_.isDefined).map(_.get)

    private val selection = Subject.publishSubject[Seq[Elem]]()

    private val menu =
        selection
                .map(_.headOption)
                .map(_.map {
                    case x: Root => Seq(UiMenuItem("Refresh", () => x.refresh()))
                    case x: Group => Seq(UiMenuItem("Refresh", () => x.refresh()))
                    case _ => Seq()
                })
                .map(_.getOrElse(Seq()))


    override def content(): UiWidget = UiPanel(layoutData, Grid(), items = Seq(
        UiTree[Elem]("grow", items = items,
            columns = Seq[UiColumn[Elem]](
                UiColumn("Group", _.name),
                UiColumn("Partition", _.partition),
                UiColumn("Current Offset", _.currentOffset),
                UiColumn("Log End Offset", _.logEndOffset),
                UiColumn("Lag", _.lag),
                UiColumn("Consumer ID", _.consumerId),
                UiColumn("Host", _.host),
                UiColumn("Client ID", _.clientId)
            ),
            selection = selection,
            subitems = _.subitems(),
            expanded = _ => false,
            hasChildren = {
                case Root(_, _) => true
                case Group(_, _) => true
                case _ => false
            }
            ,
            menu = menu
        )
    ))
}
