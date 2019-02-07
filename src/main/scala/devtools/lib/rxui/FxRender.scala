package devtools.lib.rxui

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.sun.javafx.application.LauncherImpl
import devtools.lib.rxext.ListChangeOps.{AddItems, InsertItems, RemoveItemObjs, RemoveItems, SetList}
import devtools.lib.rxext.Subject
import io.reactivex.Scheduler
import io.reactivex.Scheduler.Worker
import io.reactivex.disposables.Disposable
import javafx.application.{Application, Platform}
import javafx.beans.property.{Property, SimpleStringProperty}
import javafx.beans.value.{ChangeListener, ObservableValue}
import javafx.collections.ListChangeListener
import javafx.event.{ActionEvent, EventHandler}
import javafx.geometry.{HPos, Insets, VPos, Orientation => FxOrientation}
import javafx.scene.control.{TableColumn, _}
import javafx.scene.input._
import javafx.scene.layout._
import javafx.scene.{Node, Scene}
import javafx.stage.{Modality, Stage, StageStyle}

import scala.collection.JavaConverters._
import scala.language.implicitConversions

object FxRender {

    def linkBidirMultiSelection[T](obs: Subject[Seq[T]], selection: MultipleSelectionModel[T]): Unit = {
        for (items <- obs; if items != selection.getSelectedItems.asScala) {
            selection.clearSelection()
            for (item <- items) selection.select(item)
        }
        selection.getSelectedItems.addListener(new ListChangeListener[T] {
            override def onChanged(c: ListChangeListener.Change[_ <: T]): Unit = obs << selection.getSelectedItems.asScala.toList
        })
    }

    def linkBidirMultiSelection[T](obs: Subject[Seq[T]], selection: MultipleSelectionModel[T], applyOnSelection: Seq[T] => Unit): Unit = {
        for (items <- obs; if items != selection.getSelectedItems.asScala) {
            selection.clearSelection()
            for (item <- items) selection.select(item)
            applyOnSelection(items)
        }
        selection.getSelectedItems.addListener(new ListChangeListener[T] {
            override def onChanged(c: ListChangeListener.Change[_ <: T]): Unit = obs << selection.getSelectedItems.asScala.toList
        })
    }

    def linkBidirSingleSelect[T](obs: Subject[T], selection: SelectionModel[T]): Unit = {
        for (item <- obs; if item != selection.getSelectedItem) selection.select(item)
        selection.selectedItemProperty().addListener(new ChangeListener[T] {
            override def changed(observable: ObservableValue[_ <: T], oldValue: T, newValue: T): Unit = if (newValue != null) obs << newValue
        })
    }

    def linkBidirPropObservation[O, P](obs: Subject[O], ctrlProp: Property[P])(implicit p2o: P => O, o2p: O => P): Unit = {
        for (v <- obs; if p2o(ctrlProp.getValue) != v) ctrlProp.setValue(v)
        ctrlProp.addListener((_: P, newValue: P) => obs << newValue)
    }

    def linkBidirActObservation[O, P](obs: Subject[O], ctrlProp: ObservableValue[P], apply: O => Unit)(implicit p2o: P => O): Unit = {
        for (value <- obs; if p2o(ctrlProp.getValue) != value) apply(value)
        ctrlProp.addListener((_: P, newValue: P) => obs << newValue)
    }

    implicit def _simpleListenerToChangeListener[P](f: (P, P) => Any): ChangeListener[P] = (observable: ObservableValue[_ <: P], oldValue: P, newValue: P) => f(oldValue, newValue)

    implicit def _indexRangeToTuple(v: IndexRange): (Int, Int) = (v.getStart, v.getEnd)

    implicit def _multiSelModelToSeq[T](v: MultipleSelectionModel[T]): Seq[T] = v.getSelectedItems.asScala

    def selectionIndexes[T](src: Seq[T], selection: Set[T]): Seq[Int] =
        for ((value, idx) <- src.zipWithIndex; if selection.contains(value)) yield idx

    def using[T, R](t: => T)(expr: T => R): R = expr(t)

    def applying[T](t: => T)(expr: T => Unit): T = {val x = t; expr(x); x }

    trait Renderer[C <: Node] {
        def render(): C
    }

    class UiLabelRenderer(w: UiLabel) extends Renderer[Label] {
        override def render(): Label = {
            val c = new Label()
            for (text <- w.text) c.setText(text)
            c
        }
    }

    class UiTextRenderer(w: UiText) extends Renderer[TextInputControl] {
        override def render(): TextInputControl = {
            val c: TextInputControl = if (w.multi) new TextArea() else new TextField()
            for (obs <- w.text) linkBidirPropObservation(obs, c.textProperty())
            for (obs <- w.selection) linkBidirActObservation(obs, c.selectionProperty(), (x: (Int, Int)) => c.selectRange(x._1, x._2))
            for (editable <- w.editable) c.setEditable(editable)
            for (disabled <- w.disabled) c.setDisable(disabled)
            c
        }
    }

    class UiStyledTextRenderer(w: UiStyledText) extends Renderer[TextInputControl] {
        override def render(): TextInputControl = {
            val c: TextInputControl = if (w.multi) new TextArea() else new TextField()
            for (obs <- w.text) linkBidirPropObservation(obs, c.textProperty())
            for (obs <- w.selection) linkBidirActObservation(obs, c.selectionProperty(), (x: (Int, Int)) => c.selectRange(x._1, x._2))
            for (editable <- w.editable) c.setEditable(editable)
            for (disabled <- w.disabled) c.setDisable(disabled)
            c
        }
    }

    class UiListRenderer[T](list: UiList[T]) extends Renderer[ListView[T]] {
        override def render(): ListView[T] = {
            val c = new ListView[T]()
            for (disabled <- list.disabled) c.setDisable(disabled)
            for (multi <- list.multiSelect) c.getSelectionModel.setSelectionMode(if (multi) SelectionMode.MULTIPLE else SelectionMode.MULTIPLE)
            c.setCellFactory((param: ListView[T]) => {
                val c = new ListCell[T]()
                new ListCell[T] {
                    override def updateItem(item: T, empty: Boolean): Unit = {
                        super.updateItem(item, empty)
                        if (!empty && item != null) {
                            for (text <- list.valueProvider(item)) setText(text)
                        } else setText(null)
                    }
                }
            })
            for (items <- list.items) {
                c.getItems.clear()
                c.getItems.addAll(items.asJava)
            }
            for (obs <- list.selection) linkBidirMultiSelection(obs, c.getSelectionModel)
            for (obs <- list.onDblClick)
                c.setOnMouseClicked((event: MouseEvent) => {
                    if (event.getClickCount == 2 && event.getButton == MouseButton.PRIMARY) {
                        obs(c.getSelectionModel.getSelectedItem)
                    }
                })
            c
        }
    }

    class UiComboRenderer(combo: UiCombo) extends Renderer[ComboBox[String]] {
        override def render(): ComboBox[String] = {
            val c = new ComboBox[String]()
            for (disabled <- combo.disabled) c.setDisable(disabled)
            for (items <- combo.items) {
                val selection = c.getSelectionModel.getSelectedItem
                c.getItems.clear()
                c.getItems.addAll(items.toArray: _*)
                if (items.contains(selection)) c.getSelectionModel.select(selection)
                else c.getEditor.setText("")
            }
            for (editable <- combo.editable) c.setEditable(editable)
            for (obs <- combo.text) linkBidirPropObservation(obs, c.getEditor.textProperty())
            for (obs <- combo.selection) linkBidirSingleSelect(obs, c.getSelectionModel)
            c
        }
    }

    class UiLinkRenderer(link: UiLink) extends Renderer[Button] {
        override def render(): Button = {
            val c = new Button()
            for (disabled <- link.disabled) c.setDisable(disabled)
            c.setOnAction((event: ActionEvent) => link.onAction())
            for (text <- link.text) c.setText(text)
            c
        }
    }

    class UiButtonRenderer(button: UiButton) extends Renderer[Button] {
        override def render(): Button = {
            val c = new Button()
            for (disabled <- button.disabled) c.setDisable(disabled)
            c.setOnAction((event: ActionEvent) => button.onAction())
            for (text <- button.text) c.setText(text)
            c
        }
    }

    class UiTableRenderer[T](tableModel: UiTable[T]) extends Renderer[TableView[T]] {
        override def render(): TableView[T] = {
            val c = new TableView[T]()
            for (disabled <- tableModel.disabled) c.setDisable(disabled)
            c.getSelectionModel.setSelectionMode(SelectionMode.MULTIPLE)

            for (columns <- tableModel.columns) {
                val colsToModels: Seq[(TableColumn[T, String], UiColumn[T])] = columns
                        .map(colModel => applying(new TableColumn[T, String]()) { col =>
                            col.setText(colModel.title)
                            col.setCellValueFactory((param: TableColumn.CellDataFeatures[T, String]) => {
                                applying(new SimpleStringProperty()) { prop => for (value <- colModel.value(param.getValue)) prop.setValue(value) }
                            })
                            col.setSortable(colModel.onSort.isDefined)
                        } -> colModel)
                c.getColumns.clear()
                c.getColumns.addAll(colsToModels.map(_._1).asJavaCollection)
                c.sortPolicyProperty().set { param: TableView[T] =>
                    for ((sortCol, sortColModel) <- c.getSortOrder.asScala.headOption.flatMap(sortCol => colsToModels.find(sortCol == _._1))) {
                        sortColModel.onSort.get(sortCol.getSortType == TableColumn.SortType.ASCENDING)
                    }
                    true
                }
            }

            for (itemsOp <- tableModel.items) itemsOp match {
                case SetList(items) => c.getItems.setAll(items.asJavaCollection)
                case AddItems(items) => c.getItems.addAll(items.asJavaCollection)
                case InsertItems(index, items) => c.getItems.addAll(index, items.asJavaCollection)
                case RemoveItems(index, amount) => c.getItems.remove(index, index + amount)
                case RemoveItemObjs(items) => c.getItems.removeAll(items: _*)
            }

            for (selection <- tableModel.selection) {
                linkBidirMultiSelection(selection, c.getSelectionModel, (selectedItems: Seq[T]) => {
                    if (selectedItems.size == 1) c.scrollTo(selectedItems.head)
                })
            }

            for (handler <- tableModel.onDblClick) c.setOnMouseClicked((event: MouseEvent) =>
                if (event.getClickCount == 2 && event.getButton == MouseButton.PRIMARY)
                    handler(c.getSelectionModel.getSelectedItem))

            def menuToItems(parent: ContextMenu)(itemModel: UiMenuItem): MenuItem =
                if (itemModel.subitems.isEmpty) applying(new MenuItem(itemModel.text)) { item => item.setOnAction { evt => parent.hide(); evt.consume(); itemModel.onSelect(); } }
                else applying(new Menu(itemModel.text)) { item => item.getItems.addAll(itemModel.subitems.map(menuToItems(parent)).asJavaCollection) }

            for (menuItems <- tableModel.menu; if menuItems.nonEmpty) {
                val ctxMenuRoot = applying(new ContextMenu()) { cm =>
                    cm.getItems.addAll(menuItems.map(menuToItems(cm)).asJavaCollection)
                }
                c.setContextMenu(ctxMenuRoot)
            }
            //                for (selection <- Option(c.getSelectionModel.getSelectedItem)) {
            //                    val ctxMenuRoot = applying(new ContextMenu()) { cm =>
            //                        cm.getItems.addAll(menuItems.map(menuToItems(cm)).asJavaCollection)
            //                    }
            //                    c.setContextMenu(ctxMenuRoot)
            //prevMenu = Some(ctxMenuRoot)
            //c.getScene.getRoot.setCon
            //ctxMenuRoot.show(c, event.getScreenX, event.getScreenY)
            //                }
            //            c.setOnContextMenuRequested { event: ContextMenuEvent =>
            //                    for (prevMenu <- prevMenu) prevMenu.hide()
            //                }

            c
        }
    }

    class UiTreeRenderer[T](treeModel: UiTree[T]) extends Renderer[TreeTableView[T]] {
        override def render(): TreeTableView[T] = {
            val treeTable = new TreeTableView[T]()
            for (disabled <- treeModel.disabled) treeTable.setDisable(disabled)

            for (columns <- treeModel.columns) {
                treeTable.getColumns.addAll(columns.map(colModel => applying(new TreeTableColumn[T, String]()) { col =>
                    col.setText(colModel.title)
                    col.setCellValueFactory((param: TreeTableColumn.CellDataFeatures[T, String]) => {
                        applying(new SimpleStringProperty()) { prop => for (value <- colModel.value(param.getValue.getValue)) prop.setValue(value) }
                    })
                    col.setSortable(false)
                }).asJavaCollection)
            }

            treeTable.setShowRoot(false)
            val rootItem = new TreeItem[T]()
            treeTable.setRoot(rootItem)

            for (menuItems <- treeModel.menu)
                treeTable.setOnContextMenuRequested((event: ContextMenuEvent) =>
                    for (selection <- Option(treeTable.getSelectionModel.getSelectedItem).flatMap(x => Option(x.getValue))) {
                        val ctxMenuRoot = applying(new ContextMenu()) { cm =>
                            cm.getItems.addAll(menuItems.map(itemModel =>
                                applying(new MenuItem(itemModel.text)) { item => }).asJavaCollection)
                        }
                        treeTable.setContextMenu(ctxMenuRoot)
                        ctxMenuRoot.show(treeTable, event.getScreenX, event.getScreenY)
                    })

            class LazyTreeItem(val itemModel: T) extends TreeItem(itemModel) {

                for (items <- treeModel.subitems(itemModel)) getChildren.setAll(items.map(new LazyTreeItem(_)).asJavaCollection)
                setExpanded(treeModel.expanded(itemModel))

                override def isLeaf: Boolean = !treeModel.hasChildren(itemModel)
            }

            for (items <- treeModel.items) rootItem.getChildren.setAll(items.map(new LazyTreeItem(_)).asJavaCollection)

            val selectionModel = treeTable.getSelectionModel
            for (selection <- treeModel.selection) {
                selectionModel.getSelectedItems.addListener(new ListChangeListener[TreeItem[T]] {
                    override def onChanged(c: ListChangeListener.Change[_ <: TreeItem[T]]): Unit =
                        selection << c.getList.asScala.map(_.getValue)
                })
                for (items <- selection; if items != selectionModel.getSelectedItems.asScala.map(_.getValue)) {
                    selectionModel.clearSelection()
                }
            }

            for (handler <- treeModel.onDblClick) treeTable.setOnMouseClicked((event: MouseEvent) =>
                if (event.getClickCount == 2 && event.getButton == MouseButton.PRIMARY)
                    handler(selectionModel.getSelectedItem.getValue))

            treeTable
        }
    }

    case class GridLayoutData(margin: Int)

    case class GridLayourPos(x: Int, y: Int)

    case class GridLayoutItemData(
                                         var colspan: Int = 1,
                                         var hgrow: Boolean = false, var vgrow: Boolean = false,
                                         var hfill: Boolean = false, var vfill: Boolean = false,
                                         var halign: HPos = HPos.LEFT, var valign: VPos = VPos.TOP)

    def calculateGridLayoutData(layoutExpr: String, items: Seq[UiWidget]): (GridLayoutData, Map[UiWidget, (GridLayourPos, GridLayoutItemData)]) = {
        def tokenize(expr: String): Map[String, String] = {
            val tokens = expr.split(",")
            tokens.filterNot(_.length == 0).map(_.trim().split(" ")).map(vals =>
                if (vals.length == 1) (vals(0), null)
                else if (vals.length == 2) (vals(0), vals(1))
                else throw new RuntimeException("Expression " + expr + " can not be parsed"))
                    .toMap
        }

        def number(num: String, err: String): Int =
            try Integer.parseInt(num)
            catch {
                case _: Exception => throw new RuntimeException("Number can not be parsed from " + num + " in expr " + err)
            }

        val layout = tokenize(layoutExpr)
        val cols = number(layout.getOrElse("cols", "1"), "cols - Number of columns is illegal - in " + layoutExpr)
        val margin = number(layout.getOrElse("margin", "0"), "margin is incorrect in " + layoutExpr)

        val itemsLayout = items
                .map(x => x -> tokenize(x.layoutData))
                .map { case (item, tokens) =>
                    val data = GridLayoutItemData()
                    data.halign = HPos.LEFT
                    data.valign = VPos.CENTER
                    tokens.flatMap {
                        case ("grow", _) => Seq("grabx" -> null, "graby" -> null, "fillx" -> null, "filly" -> null)
                        case ("growx", _) => Seq("grabx" -> null, "fillx" -> null)
                        case ("growy", _) => Seq("graby" -> null, "filly" -> null)
                        case ("grab", _) => Seq("grabx" -> null, "graby" -> null)
                        case ("fill", _) => Seq("fillx" -> null, "filly" -> null)
                        case (k, v) => Seq(k -> v)
                    }.foreach {
                        case ("colspan", spanStr) => number(spanStr, "colspan can not be parsed from value ")
                        case ("grabx", _) => data.hgrow = true
                        case ("graby", _) => data.vgrow = true
                        case ("fillx", _) => data.hfill = true
                        case ("filly", _) => data.vfill = true
                        case ("halign", ha) =>
                            data.halign = if (ha == "L") HPos.LEFT else if (ha == "C") HPos.CENTER else if (ha == "R") HPos.RIGHT
                            else throw new Exception("No HPOS " + ha + " supported")
                        case ("valign", va) =>
                            data.valign = if (va == "T") VPos.TOP else if (va == "C") VPos.CENTER else if (va == "B") VPos.BOTTOM
                            else throw new Exception("No VPOS " + va + " supported")
                        case ("w", width) =>
                        case (k, v) => throw new Exception("No token " + k + " with value " + v + " is supported")
                    }
                    item -> data
                }
                .foldLeft((GridLayourPos(0, 0), List[(UiWidget, (GridLayourPos, GridLayoutItemData))]())) { case ((pos, layouts), (item, itemLayoutData)) =>
                    (if (pos.x + itemLayoutData.colspan >= cols) GridLayourPos(0, pos.y + 1)
                    else GridLayourPos(pos.x + itemLayoutData.colspan, pos.y)) ->
                            (item -> (pos -> itemLayoutData) :: layouts)
                }
                ._2
                .toMap
        (GridLayoutData(margin = margin), itemsLayout)
    }

    class UiPanelRenderer[T](panel: UiPanel)(implicit renderers: FxRenderers) extends Renderer[Pane] {
        override def render(): Pane = {
            val c = new GridPane()
            //c.setGridLinesVisible(false)
            //c.setBorder(Border.EMPTY)

            for (items <- panel.items) {

                val (paneLayout, itemLayouts) = calculateGridLayoutData(panel.layout.asInstanceOf[Grid].markup, items)
                c.getChildren.clear()
                val paneCItems: Iterable[(UiWidget, Node)] = for (item <- items) yield item -> renderers.renderer(item).render()
                paneCItems.foreach { case (item, cItem) =>
                    val l = itemLayouts(item)
                    GridPane.setFillWidth(cItem, l._2.hfill)
                    GridPane.setFillHeight(cItem, l._2.vfill)
                    GridPane.setHgrow(cItem, if (l._2.hgrow) Priority.ALWAYS else null)
                    GridPane.setVgrow(cItem, if (l._2.vgrow) Priority.ALWAYS else null)
                    GridPane.setHalignment(cItem, l._2.halign)
                    GridPane.setValignment(cItem, l._2.valign)
                    GridPane.setMargin(cItem, new Insets(paneLayout.margin, paneLayout.margin, paneLayout.margin, paneLayout.margin))

                    c.add(cItem, l._1.x, l._1.y, l._2.colspan, 1)
                }
            }

            c
        }
    }

    class UiTabPanelRenderer(tabPanelModel: UiTabPanel)(implicit renderers: FxRenderers) extends Renderer[Node] {
        override def render(): Node = {
            val pane = new TabPane()
            for (tabs <- tabPanelModel.tabs) pane.getTabs.setAll(
                tabs.map { tabModel =>
                    val tab = new Tab()
                    for (label <- tabModel.label) tab.setText(label)
                    for (content <- tabModel.content) tab.setContent(renderers.renderer(content).render())
                    tab.setClosable(false)
                    tab
                }.asJavaCollection)
            pane
        }
    }

    class UiTabPanelListOpsRenderer[T](tabPanelModel: UiTabPanelExt[T])(implicit renderers: FxRenderers) extends Renderer[Node] {
        override def render(): Node = {
            val pane = new TabPane()
            var itemsToContent = Map[T, Tab]()
            val tabModelToTabs: Seq[T] => java.util.Collection[Tab] = _.map { itemModel =>
                val tabModel = tabPanelModel.tab(itemModel)
                val tab = new Tab()
                for (label <- tabModel.label) tab.setText(label)
                for (content <- tabModel.content) tab.setContent(renderers.renderer(content).render())
                tab.setClosable(tabPanelModel.closeable)
                itemsToContent += itemModel -> tab
                tab.setOnClosed { _ => tabPanelModel.onClose(itemModel) }
                tab
            }.asJavaCollection
            for (itemsOp <- tabPanelModel.tabs) itemsOp match {
                case SetList(itemModels) => pane.getTabs.setAll(tabModelToTabs(itemModels))
                case AddItems(itemModels) => pane.getTabs.addAll(tabModelToTabs(itemModels))
                case InsertItems(index, itemModels) => pane.getTabs.addAll(index, tabModelToTabs(itemModels))
                case RemoveItems(index, amount) => pane.getTabs.remove(index, index + amount)
                case RemoveItemObjs(itemModels) => pane.getTabs.removeAll(itemsToContent.filter(pair => itemModels.contains(pair._1)).values.asJavaCollection)
            }
            DraggableTabs.decorate(pane)
            pane
        }
    }

    object DraggableTabs {

        def decorate[T](tabPane: TabPane): Unit = {
            val decorId = UUID.randomUUID().toString
            var currentDraggingTab: Tab = null

            def decorateTab(tab: Tab): Unit = {
                for (txt <- Option(tab.getText).filter(!_.isEmpty)) {
                    val label = new Label(txt, tab.getGraphic)
                    tab.setText(null)
                    tab.setGraphic(label)
                }
                val graphic = tab.getGraphic
                graphic.setOnDragDetected { e =>
                    val dragboard = graphic.startDragAndDrop(TransferMode.MOVE)
                    val clipContent = new ClipboardContent()
                    clipContent.putString(decorId)
                    dragboard.setContent(clipContent)
                    dragboard.setDragView(graphic.snapshot(null, null))
                    currentDraggingTab = tab
                }
                graphic.setOnDragOver { e =>
                    if (decorId == e.getDragboard.getString &&
                            currentDraggingTab != null &&
                            currentDraggingTab.getGraphic != graphic) {
                        e.acceptTransferModes(TransferMode.MOVE)
                    }
                }
                graphic.setOnDragDropped { e =>
                    if (decorId == e.getDragboard.getString &&
                            currentDraggingTab != null &&
                            currentDraggingTab.getGraphic != graphic) {
                        val idx = tabPane.getTabs.indexOf(tab)
                        tabPane.getTabs.remove(currentDraggingTab)
                        tabPane.getTabs.add(idx, currentDraggingTab)
                        tabPane.getSelectionModel.select(currentDraggingTab)
                    }
                }
                graphic.setOnDragDone(_ => currentDraggingTab = null)
            }

            def undecorateTab(tab: Tab): Unit = {
                tab.getGraphic.setOnDragDetected(null)
                tab.getGraphic.setOnDragOver(null)
                tab.getGraphic.setOnDragDropped(null)
                tab.getGraphic.setOnDragDone(null)
            }

            tabPane.getTabs.forEach(decorateTab)
            tabPane.getTabs.addListener(new ListChangeListener[Tab] {
                override def onChanged(c: ListChangeListener.Change[_ <: Tab]): Unit = {
                    while (c.next()) {
                        if (c.wasAdded()) c.getAddedSubList.forEach(decorateTab)
                        if (c.wasRemoved()) c.getRemoved.forEach(undecorateTab)
                    }
                }
            })
        }
    }

    class UiSplitPaneRenderer(paneModel: UiSplitPane)(implicit renderers: FxRenderers) extends Renderer[Node] {
        override def render(): Node = {
            val pane = new SplitPane()
            pane.setOrientation(if (paneModel.orientation == UiHoriz) FxOrientation.HORIZONTAL else FxOrientation.VERTICAL)
            pane.getItems.addAll(
                renderers.renderer(paneModel.els._1).render(),
                renderers.renderer(paneModel.els._2).render())
            pane.setDividerPosition(0, paneModel.proportion.toDouble / 100)
            pane
        }
    }

    class UiComponentRenderer(comp: UiComponent)(implicit renderers: FxRenderers) extends Renderer[Node] {
        override def render(): Node = {
            val widget = comp.content()
            renderers.renderer(widget).render()
        }
    }

    trait FxRenderers extends UiRenderer {

        def renderer(w: UiWidget)(implicit renderers: FxRenderers): Renderer[_ <: Node]

        def uiScheduler(): Scheduler = () => new Worker {
            private var disposed = false

            override def schedule(run: Runnable, delay: Long, unit: TimeUnit): Disposable = {/*println("Do async"); */ Platform.runLater(run); this }

            override def dispose(): Unit = disposed = true

            override def isDisposed: Boolean = disposed
        }

        override def runApp(root: UiWidget): Unit = {
            FxRender.rootContent = root
            LauncherImpl.launchApplication(classOf[App], Array())
        }

        override def runModal(content: UiWidget, hideTitle: Boolean = false, close: Option[Subject[_ >: Unit]] = None): Unit = {
            val dialog = new Stage(if (hideTitle) StageStyle.UNDECORATED else StageStyle.DECORATED)

            applyContentToState(content, dialog, fullScreen = false)(FxRender.renderers)

            for (obs <- close; _ <- obs) {
                dialog.close()
            }

            dialog.initOwner(FxRender.primaryStage)
            dialog.initModality(Modality.WINDOW_MODAL)
            dialog.showAndWait()
            for (x <- close) x << {}
        }

        override def alert(alertType: AlertType, message: String): Unit = {
            val alert = new Alert(Alert.AlertType.ERROR, message)
            alert.showAndWait()
        }

    }

    implicit case object DefaultFxRenderes extends FxRenderers {

        override def renderer(w: UiWidget)(implicit renderers: FxRenderers): Renderer[_ <: Node] = w match {
            case x: UiLabel => new UiLabelRenderer(x)
            case x: UiText => new UiTextRenderer(x)
            case x: UiStyledText => new UiStyledTextRenderer(x)
            case x: UiButton => new UiButtonRenderer(x)
            case x: UiLink => new UiLinkRenderer(x)
            case x: UiCombo => new UiComboRenderer(x)
            case x: UiList[_] => new UiListRenderer(x)
            case x: UiTable[_] => new UiTableRenderer(x)
            case x: UiTree[_] => new UiTreeRenderer(x)
            case x: UiPanel => new UiPanelRenderer(x)
            case x: UiTabPanel => new UiTabPanelRenderer(x)
            case x: UiTabPanelExt[_] => new UiTabPanelListOpsRenderer(x)
            case x: UiSplitPane => new UiSplitPaneRenderer(x)
            case x: UiComponent => new UiComponentRenderer(x)
        }

        override def runApp(root: UiWidget): Unit = {
            FxRender.renderers = this
            super.runApp(root)
        }

    }

    def grabFullContent(widget: UiWidget)(implicit renderers: FxRenderers): Pane = {
        val pane = new GridPane()
        val contentNode = renderers.renderer(widget).render()
        GridPane.setHgrow(contentNode, Priority.ALWAYS)
        GridPane.setVgrow(contentNode, Priority.ALWAYS)
        GridPane.setFillWidth(contentNode, true)
        GridPane.setFillHeight(contentNode, true)
        pane.add(contentNode, 0, 0)
        pane
    }

    def applyContentToState(widget: UiWidget, stage: Stage, fullScreen: Boolean)(implicit renderers: FxRenderers): Unit = {
        val rootPane = grabFullContent(widget)
        if (fullScreen) {
            rootPane.setPrefSize(java.awt.Toolkit.getDefaultToolkit.getScreenSize.width * 0.9, java.awt.Toolkit.getDefaultToolkit.getScreenSize.height * 0.9)
            stage.setMaximized(true)
        }
        val scene = new Scene(rootPane)
        val url = classOf[App].getResource("/css.css")
        val cssLocation = url.toExternalForm
        scene.getStylesheets.add(cssLocation)
        stage.setScene(scene)
    }

    var rootContent: UiWidget = _
    var renderers: FxRenderers = _
    var primaryStage: Stage = _

    class App extends Application {
        override def start(primaryStage: Stage): Unit = {
            FxRender.primaryStage = primaryStage
            applyContentToState(FxRender.rootContent, primaryStage, fullScreen = true)(FxRender.renderers)
            primaryStage.show()
        }
    }


}
