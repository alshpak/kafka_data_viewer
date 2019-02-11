import devtools.lib.rxext.ListChangeOps.{ListChangeOp, SetList}
import devtools.lib.rxext.{Observable, Subject}
import devtools.lib.rxui.UiImplicits._
import devtools.lib.rxui._

object MemoryTest {
    def main(args: Array[String]): Unit = {

    }
}

case class TabModel()

case class SomeItem(text: String = "")

class MemoryTestApp(val layoutData: String = "") extends UiObservingComponent {

    private val tabs = Subject.behaviorSubject[ListChangeOp[TabModel]](SetList[TabModel](Seq()))

    override def content(): UiWidget = UiPanel("", Grid(), items = Seq(
        UiPanel("growx", Grid(), items = Seq(
            UiButton(text = "Add Tab")
        )),
        UiTabPanelExt[TabModel]("grow",
            tabs = tabs,
            tab = model => UiTab("Tab", content = new MemoryTestTabContent("", items = ???))
        )
    ))
}

class MemoryTestTabContent(val layoutData: String = "", items: Observable[SomeItem]) extends UiComponent {
    override def content(): UiWidget = ???
}