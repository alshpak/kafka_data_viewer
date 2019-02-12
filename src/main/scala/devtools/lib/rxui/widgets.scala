package devtools.lib.rxui

import scala.collection.mutable
import scala.language.implicitConversions

import io.reactivex.Scheduler
import io.reactivex.disposables.Disposable

import devtools.lib.rxext.ListChangeOps.ListChangeOp
import devtools.lib.rxext.Observable.{empty, just}
import devtools.lib.rxext.{Observable, Subject}
import devtools.lib.rxui.UiImplicits._

trait Listanable[T] {
    def foreach(t: T => Unit): Unit

    def withFilter(predicate: T => Boolean): Listanable[T]
}

object Listanable {
    def empty[T](): Listanable[T] = new Listanable[T]() {
        override def foreach(t: T => Unit): Unit = (t: T => Unit) => Unit

        override def withFilter(predicate: T => Boolean): Listanable[T] = empty()
    }

}

class DisposeStore {

    private val resources = mutable.ArrayBuffer[Disposable]()

    def apply[T](obs: Observable[T]): Listanable[T] = new Listanable[T]() {
        override def foreach(t: T => Unit): Unit = {val resource = obs.subscribe(t); resources += resource }

        override def withFilter(predicate: T => Boolean): Listanable[T] = {
            val filteredObs = obs.withFilter(predicate)
            apply(filteredObs)
        }
    }


    def dispose(): Unit = {resources foreach (_.dispose()); resources clear() }
}


trait UiWidget {val layoutData: String}

trait Layout

sealed trait AlertType

case object ErrorAlert extends AlertType

case class Grid(markup: String = "") extends Layout

case class UiMenu(items: Seq[UiMenuItem])

case class UiMenuItem(
                             text: String,
                             onSelect: () => Unit = () => Unit,
                             subitems: Seq[UiMenuItem] = Seq(),
                             disabled: Observable[Boolean] = false)

case class UiLabel(layoutData: String = "", text: Observable[String] = empty()) extends UiWidget

case class UiText(layoutData: String = "",
                  multi: Boolean = false,
                  text: Option[Subject[String]] = None,
                  selection: Option[Subject[(Int, Int)]] = None,
                  editable: Observable[Boolean] = true,
                  disabled: Observable[Boolean] = false) extends UiWidget

case class UiStyledText(layoutData: String = "",
                        multi: Boolean = false,
                        text: Option[Subject[String]] = None,
                        selection: Option[Subject[(Int, Int)]] = None,
                        editable: Observable[Boolean] = true,
                        disabled: Observable[Boolean] = false) extends UiWidget

case class UiList[T](layoutData: String = "",
                     items: Observable[Seq[T]] = empty[Seq[T]](),
                     valueProvider: T => Observable[String] = (x: T) => x.toString,
                     selection: Option[Subject[Seq[T]]] = None,
                     onDblClick: Option[T => Unit] = None,
                     multiSelect: Observable[Boolean] = false,
                     menu: Observable[Seq[UiMenuItem]] = empty(),
                     disabled: Observable[Boolean] = false) extends UiWidget

case class UiCombo(layoutData: String = "",
                   items: Observable[Seq[String]] = empty(),
                   text: Option[Subject[String]] = None,
                   selection: Option[Subject[String]] = None,
                   editable: Observable[Boolean] = true,
                   disabled: Observable[Boolean] = false) extends UiWidget

case class UiLink(layoutData: String = "",
                  text: Observable[String] = empty(),
                  onAction: () => Unit = () => Unit,
                  disabled: Observable[Boolean] = false) extends UiWidget

case class UiButton(layoutData: String = "",
                    text: Observable[String] = empty(),
                    onAction: () => Unit = () => Unit,
                    disabled: Observable[Boolean] = false,
                    defaultButton: Boolean = false,
                    cancelButton: Boolean = false
                   ) extends UiWidget

case class UiSeparator(layoutData: String = "",
                       orientation: UiOrientation) extends UiWidget

case class UiColumn[T](
                              title: String,
                              value: T => Observable[String] = (_: T) => empty(),
                              onSort: Option[Boolean => Unit] = None)

case class UiTable[T](layoutData: String = "",
                      items: Observable[_ <: ListChangeOp[T]] = empty(),
                      columns: Observable[Seq[UiColumn[T]]] = empty[Seq[UiColumn[T]]](),
                      selection: Option[Subject[Seq[T]]] = None,
                      onDblClick: Option[T => Unit] = None,
                      disabled: Observable[Boolean] = false,
                      multiSelect: Observable[Boolean] = false,
                      menu: Observable[Seq[UiMenuItem]] = empty()
                     ) extends UiWidget

case class UiTree[T](layoutData: String = "",
                     items: Observable[Seq[T]] = empty[Seq[T]](),
                     columns: Observable[Seq[UiColumn[T]]] = empty[Seq[UiColumn[T]]](),
                     selection: Option[Subject[Seq[T]]] = None,
                     expanded: T => Boolean = (x: T) => false,
                     subitems: T => Observable[Seq[T]] = (x: T) => just(Seq()),
                     hasChildren: T => Boolean = (x: T) => false,
                     onDblClick: Option[T => Unit] = None,
                     menu: Observable[Seq[UiMenuItem]] = empty(),
                     disabled: Observable[Boolean] = false
                    ) extends UiWidget

case class UiTab(label: Observable[String] = empty(), content: Observable[UiWidget])

case class UiTabPanel(layoutData: String = "",
                      tabs: Observable[Seq[UiTab]] = empty()) extends UiWidget

case class UiTabPanelExt[T](layoutData: String = "",
                            tabs: Observable[_ <: ListChangeOp[T]] = empty(),
                            tab: T => UiTab,
                            closeable: Boolean = false,
                            onClose: T => Any = (_: T) => Unit,
                            selection: Option[Subject[T]] = None,
                            moveTabs: Boolean = false,
                            onTabsOrdered: Seq[(T, Int)] => Any = (_: Seq[(T, Int)]) => Unit
                           ) extends UiWidget

trait UiOrientation

case object UiHoriz extends UiOrientation

case object UiVert extends UiOrientation

case class UiSplitPane(layoutData: String = "",
                       orientation: UiOrientation,
                       proportion: Int,
                       els: (UiWidget, UiWidget)) extends UiWidget

case class UiPanel(layoutData: String = "",
                   layout: Layout,
                   visible: Observable[Boolean] = empty(),
                   items: Observable[Seq[UiWidget]] = just(Seq())) extends UiWidget

object UiImplicits {

    implicit def anyToJust[T](t: T): Observable[T] = Observable.just(t)

    implicit def optToSome[T](t: T): Option[T] = Some(t)

    implicit def optAnyToJust[T](t: T): Option[Observable[T]] = Some(Observable.just(t))

    implicit def functionToJustObservable[T](t: T => String): T => Observable[String] = x => just(t(x))

    implicit def unitSubjectToConsumer(s: Subject[Unit]): () => Unit = () => s << Unit

    implicit def subjectTo1ArgConsumer[T](s: Subject[T]): Some[T => Unit] = Some(x => s << x)

    implicit class ContextSubscribe[T](subj: Subject[T]) {
        def <<<(obs: Observable[T])($: Observable[T] => Listanable[T]): Unit = for (item <- $(obs)) subj << item
    }

    implicit class ContextObserving[T](obs: Observable[T]) {

        def asPublishSubject($: Observable[T] => Listanable[T]): Subject[T] = {val subject = Subject.publishSubject[T](); (subject <<< obs) ($); subject }
    }

}

trait UiComponent extends UiWidget {

    def content(): UiWidget

}

trait UiObservingComponent extends UiComponent {

    private val disposeStore = new DisposeStore()

    def $[T](obs: Observable[T]): Listanable[T] = disposeStore(obs)

    def dispose(): Unit = disposeStore.dispose()
}

trait UiRenderer {

    def runApp(root: UiWidget, postAction: UiRenderer => Unit = null): Unit

    def runModal(content: UiWidget, hideTitle: Boolean = false, close: Option[Subject[_ >: Unit]] = None): Unit

    def alert(alertType: AlertType, message: String): Unit

    def uiScheduler(): Scheduler
}