package devtools.kafka_data_viewer.ui

import devtools.kafka_data_viewer.KafkaDataViewer.ConnectionDefinition
import devtools.lib.rxext.Observable.combineLatest
import devtools.lib.rxext.Subject.{behaviorSubject, publishSubject}
import devtools.lib.rxext.{BehaviorSubject, Observable, Subject}
import devtools.lib.rxui.UiImplicits._
import devtools.lib.rxui._

import scala.language.postfixOps

class KafkaConnectionsListPane(val layoutData: String = "",
                               connections: BehaviorSubject[Seq[ConnectionDefinition]],
                               onConnect: Subject[ConnectionDefinition]
                              )(implicit uiRenderer: UiRenderer) extends UiObservingComponent {

    private val onAdd = publishSubject[Unit]()
    private val onChange = publishSubject[Unit]()
    private val onRemove = publishSubject[Unit]()
    private val connSelected = behaviorSubject(Seq[ConnectionDefinition]())

    for (_ <- $(onAdd)) {
        val applyHandle = publishSubject[Unit]()
        val closeHandle = publishSubject[Unit]()
        val newConnection = ConnectionDefinition()
        for (conn <- $(applyHandle)) connections << connections.value :+ newConnection
        uiRenderer.runModal(new ConfigureConnectionWindow("", newConnection, applyHandle, closeHandle), close = closeHandle)
    }

    for (_ <- $(onChange); selectedCon <- connSelected.value.headOption) {
        val applyHandle = publishSubject[Unit]()
        val closeHandle = publishSubject[Unit]()
        uiRenderer.runModal(new ConfigureConnectionWindow("", selectedCon, applyHandle, closeHandle), close = closeHandle)
    }

    for (_ <- $(onRemove); selectedCon <- connSelected.value.headOption) {
        connections << connections.value.filterNot(selectedCon ==)
    }

    private def connMenu(conn: ConnectionDefinition): Seq[UiMenuItem] = Seq(
        UiMenuItem(text = "Connect (dbl-click)", onSelect = () => onConnect << conn),
        UiMenuItem(text = "Edit", onSelect = onChange),
        UiMenuItem(text = "Remove", onSelect = onRemove)
    )

    private val menu: Observable[Seq[UiMenuItem]] = connSelected.map(connSeq =>
        if (connSeq.size == 1) connMenu(connSeq.head) else Seq()
    )

    override def content(): UiWidget = UiPanel("", Grid(), items = Seq(
        UiPanel("", Grid("margin 5"), items = Seq(UiButton(text = "Add Connection", onAction = onAdd))),
        UiList[ConnectionDefinition]("grow",
            items = connections,
            valueProvider = con => combineLatest(con.name, con.kafkaHost)
                    .map(x => s"${x._1} (${x._2})"),
            selection = connSelected,
            menu = menu,
            onDblClick = onConnect
        )
    ))
}

class ConfigureConnectionWindow(
                                       val layoutData: String = "",
                                       conn: ConnectionDefinition,
                                       onApply: Subject[Unit],
                                       onClose: Subject[Unit]
                               ) extends UiObservingComponent {
    private val onOk = publishSubject[Unit]()
    for (_ <- $(onOk)) {
        conn.name << name.value.trim
        conn.kafkaHost << host.value.trim
        onApply onNext Unit
        onClose onNext Unit
    }
    private val name = behaviorSubject(conn.name.value)
    private val host = behaviorSubject[String](conn.kafkaHost.value)

    private val applyAllowed = Observable.combineLatest(name, host)
            .map(x => !x._1.trim.isEmpty && !x._2.trim.isEmpty)

    override def content(): UiWidget = UiPanel(layoutData, Grid("cols 2,margin 5"), items = Seq(
        UiLabel(text = "Connection Name"),
        UiText("growx", text = name),
        UiLabel(text = "Kafka Host"),
        UiText("growx", text = host),
        UiSeparator("colspan 2, growx, graby, valign T", orientation = UiHoriz),
        UiPanel("colspan 2, halign R", Grid("cols 2,margin 2"), items = Seq(
            UiButton(text = "Cancel", onAction = onClose, cancelButton = true),
            UiButton(text = "Ok", onAction = onOk, disabled = applyAllowed.map(!_), defaultButton = true)
        ))
    ))
}
