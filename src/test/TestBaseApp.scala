import devtools.lib.rxui.FxRender.DefaultFxRenderes
import devtools.lib.rxui.UiImplicits._
import devtools.lib.rxui.{UiComponent, UiLabel, UiWidget}

object TestBaseApp {
    def main(args: Array[String]): Unit = {
        DefaultFxRenderes.runApp(new TestBaseApp(""))
    }
}

class TestBaseApp(val layoutData: String = "") extends UiComponent {
    override def content(): UiWidget = UiLabel(text = "Test")
}