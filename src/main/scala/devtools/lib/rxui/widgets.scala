package devtools.lib.rxui

import devtools.lib.rxext.ListChangeOps.ListChangeOp
import devtools.lib.rxext.Observable.{empty, just}
import devtools.lib.rxext.{Observable, Subject}
import io.reactivex.Scheduler

import scala.language.implicitConversions

trait UiWidget {val layoutData: String}

trait Layout

case class Grid(markup: String = "") extends Layout

case class UiMenu(items: Seq[UiMenuItem])

case class UiMenuItem(text: String, onSelect: () => Unit, subitems: Seq[UiMenuItem] = Seq())

case class UiLabel(layoutData: String = "", text: Observable[String] = empty()) extends UiWidget

case class UiText(layoutData: String = "",
                  multi: Boolean = false,
                  text: Option[Subject[String]] = None,
                  selection: Option[Subject[(Int, Int)]] = None) extends UiWidget

case class UiStyledText(layoutData: String = "",
                        multi: Boolean = false,
                        t: Either[Observable[String], Subject[String]] = Left(just("")),
                        text: Option[Subject[String]] = None,
                        editable: Observable[Boolean] = just(true),
                        selection: Option[Subject[(Int, Int)]] = None) extends UiWidget

case class UiList[T](layoutData: String = "",
                     items: Observable[Seq[T]] = empty[Seq[T]](),
                     valueProvider: T => String = (x: T) => x.toString,
                     selection: Option[Subject[Seq[T]]] = None,
                     onDblClick: Option[_ <: Subject[_ >: T]] = None,
                     multi: Boolean = false) extends UiWidget

case class UiCombo(layoutData: String = "",
                   items: Observable[Seq[String]] = empty(),
                   text: Option[Subject[String]] = None,
                   selection: Option[Subject[String]] = None,
                   enabled: Option[Subject[Boolean]] = None) extends UiWidget

case class UiLink(layoutData: String = "",
                  text: Observable[String] = empty(),
                  onAction: Option[Subject[Unit]] = None) extends UiWidget

case class UiButton(layoutData: String = "",
                    text: Observable[String] = empty(),
                    onAction: Option[_ <: Subject[_ >: Any]] = None,
                    enabled: Observable[Boolean] = just(true)) extends UiWidget

case class UiColumn[T](
                              title: String,
                              valueProvider: T => String = (x: T) => x.toString,
                              onSort: Option[Subject[Boolean]] = None)

case class UiTable[T](layoutData: String = "",
                      items: Observable[_ <: ListChangeOp[T]] = empty(),
                      columns: Seq[UiColumn[T]] = Seq(),
                      selection: Option[Subject[Seq[T]]] = None,
                      onDblClick: Option[Subject[T]] = None,
                      disabled: Option[Observable[Boolean]] = None
                     ) extends UiWidget

case class UiTree[T](layoutData: String = "",
                     items: Observable[Seq[T]] = empty[Seq[T]](),
                     columns: Seq[UiColumn[T]] = Seq(),
                     expanded: T => Boolean = (x: T) => false,
                     subitems: T => Observable[Seq[T]] = (x: T) => just(Seq()),
                     hasChildren: T => Boolean = (x: T) => false,
                     selection: Option[Subject[Seq[T]]] = None,
                     onDblClick: Option[Subject[T]] = None,
                     menu: Option[T => Seq[UiMenuItem]] = None
                    ) extends UiWidget

case class UiTab(label: Observable[String] = empty(), content: UiWidget)

case class UiTabPanel(layoutData: String = "",
                      tabs: Observable[Seq[UiTab]] = empty(),
                      closeable: Boolean = false,
                      onClose: UiWidget => Unit = _ => Unit) extends UiWidget


case class UiTabPanelListOps[T](layoutData: String = "",
                                tabs: Observable[_ <: ListChangeOp[T]] = empty[ListChangeOp[T]](),
                                tab: T => UiTab,
                                closeable: Boolean = false,
                                onClose: T => Any = (_: T) => Unit,
                                moveTabs: Boolean = false) extends UiWidget

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

    //    implicit def asSubject[T](o: Observable[T]): Subject[T] = {val subject = Subject.publishSubject[T](); subject <<< o; subject }
    //
    //    implicit def asOptSubject[T](o: Observable[T]): Option[Subject[T]] = Some(asSubject(o))

}

trait UiComponent extends UiWidget {

    def content(): UiWidget

}

trait UiRenderer {

    def runApp(root: UiWidget): Unit

    def runModal(content: UiWidget, hideTitle: Boolean = false, close: Option[Subject[_ >: Any]] = None): Unit

    def uiScheduler(): Scheduler
}