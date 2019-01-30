package devtools.kafka_data_viewer.ui

import devtools.kafka_data_viewer.KafkaDataViewer.ConnectionDefinition
import devtools.lib.rxext.Subject.{behaviorSubject, publishSubject}
import devtools.lib.rxext.{BehaviorSubject, Subject}
import devtools.lib.rxui.UiImplicits._
import devtools.lib.rxui._

import scala.language.postfixOps

class KafkaConnectionsListPane(val layoutData: String = "",
                               connections: BehaviorSubject[Seq[ConnectionDefinition]],
                               onConnect: Subject[ConnectionDefinition],
                              )(implicit uiRenderer: UiRenderer) extends UiComponent {

    private val onAdd = publishSubject[Unit]()
    private val onChange = publishSubject[Unit]()
    private val onRemove = publishSubject[Unit]()
    private val connSelected = behaviorSubject(Seq[ConnectionDefinition]())

    for (_ <- onAdd) {
        val applyHandle = publishSubject[ConnectionDefinition]()
        val closeHandle = publishSubject[Unit]()
        for (conn <- applyHandle) connections << (connections.value :+ conn)
        uiRenderer.runModal(new ConfigureConnectionWindow("", ConnectionDefinition("", "", ""), applyHandle, closeHandle))
    }

    for (_ <- onChange; selectedCon <- connSelected.value.headOption) {
        val applyHandle = publishSubject[ConnectionDefinition]()
        val closeHandle = publishSubject[Unit]()
        for (conn <- applyHandle) connections << (connections.value :+ conn).filterNot(selectedCon ==)
        uiRenderer.runModal(new ConfigureConnectionWindow("", selectedCon, applyHandle, closeHandle), close = closeHandle)
    }

    for (_ <- onRemove; selectedCon <- connSelected.value.headOption) {
        connections << connections.value.filterNot(selectedCon ==)
    }

    override def content(): UiWidget = UiPanel("", Grid(), items = Seq(
        UiLabel("growx", text = "Double Click to connect"),
        UiList[ConnectionDefinition]("grow", items = connections, valueProvider = con => con.kafkaHost, selection = connSelected, onDblClick = onConnect),
        UiPanel("", Grid("cols 3"), items = Seq(
            UiButton(text = "Add", onAction = onAdd),
            UiButton(text = "Change", onAction = onChange),
            UiButton(text = "Remove", onAction = onRemove)
        ))
    ))
}

class ConfigureConnectionWindow(
                                       val layoutData: String = "",
                                       conn: ConnectionDefinition,
                                       onApply: Subject[ConnectionDefinition],
                                       onClose: Subject[Unit]
                               ) extends UiComponent {
    private val onOk = publishSubject[Unit]()
    for (_ <- onOk) {
        onApply onNext ConnectionDefinition(name = name.value, kafkaHost = host.value, zoo = zoo.value, topicSettings = conn.topicSettings)
        onClose onNext Unit
    }
    private val name = behaviorSubject(conn.name)
    private val host = behaviorSubject[String](conn.kafkaHost)
    private val zoo = behaviorSubject[String](Option(conn.zoo).getOrElse(""))

    override def content(): UiWidget = UiPanel(layoutData, Grid("cols 2"), items = Seq(
        UiLabel(text = "Connection Name"),
        UiText("growx", text = name),
        UiLabel(text = "Kafka Host"),
        UiText("growx", text = host),
        UiLabel(text = "Zoo Host"),
        UiText("growx", text = zoo),
        UiPanel("colspan 2", Grid("cols 2"), items = Seq(
            UiButton(text = "Ok", onAction = onOk),
            UiButton(text = "Cancel", onAction = onClose)
        ))
    ))
}
