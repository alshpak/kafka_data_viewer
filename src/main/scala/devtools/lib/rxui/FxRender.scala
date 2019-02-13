package devtools.lib.rxui

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.{implicitConversions, postfixOps}

import com.sun.javafx.application.LauncherImpl
import io.reactivex.Scheduler
import io.reactivex.Scheduler.Worker
import io.reactivex.disposables.Disposable
import javafx.application.{Application, Platform}
import javafx.beans.property.{Property, SimpleStringProperty}
import javafx.beans.value.{ChangeListener, ObservableValue}
import javafx.collections.{ListChangeListener, ObservableList}
import javafx.event.ActionEvent
import javafx.geometry.{HPos, Insets, VPos, Orientation => FxOrientation}
import javafx.scene.control.TabPane.TabClosingPolicy
import javafx.scene.control.{Alert, Button, ComboBox, ContextMenu, Control, IndexRange, Label, ListCell, ListView, Menu, MenuItem, MultipleSelectionModel, SelectionMode, SelectionModel, Separator, SplitPane, Tab, TabPane, TableColumn, TableView, TextArea, TextField, TextInputControl, TreeItem, TreeTableColumn, TreeTableView}
import javafx.scene.input.{ClipboardContent, ContextMenuEvent, MouseButton, MouseEvent, TransferMode}
import javafx.scene.layout.{Border, BorderStroke, BorderStrokeStyle, BorderWidths, CornerRadii, GridPane, Pane, Priority}
import javafx.scene.paint.Color
import javafx.scene.{Node, Scene}
import javafx.stage.{Modality, Stage, StageStyle}

import devtools.lib.rxext.ListChangeOps.{AddItems, InsertItems, RemoveItemObjs, RemoveItems, SetList}
import devtools.lib.rxext.{Observable, Subject}
import devtools.lib.rxui.FxRender.primaryStage

object FxRender {

    def linkBidirMultiSelection[T](obs: Subject[Seq[T]], selection: MultipleSelectionModel[T])($: Observable[Seq[T]] => Listanable[Seq[T]]): Unit = {
        for (items: Seq[T] <- $(obs); if items != selection.getSelectedItems.asScala) {
            selection.clearSelection()
            for (item <- items) selection.select(item)
        }
        selection.getSelectedItems.addListener(new ListChangeListener[T] {
            override def onChanged(c: ListChangeListener.Change[_ <: T]): Unit = obs << selection.getSelectedItems.asScala.toList
        })
    }

    def linkBidirMultiSelection[T](obs: Subject[Seq[T]], selection: MultipleSelectionModel[T], applyOnSelection: Seq[T] => Unit)($: Observable[Seq[T]] => Listanable[Seq[T]]): Unit = {
        for (items: Seq[T] <- $(obs); if items != selection.getSelectedItems.asScala) {
            selection.clearSelection()
            for (item <- items) selection.select(item)
            applyOnSelection(items)
        }
        selection.getSelectedItems.addListener(new ListChangeListener[T] {
            override def onChanged(c: ListChangeListener.Change[_ <: T]): Unit = obs << selection.getSelectedItems.asScala.toList
        })
    }

    def linkBidirSingleSelect[T](obs: Subject[T], selection: SelectionModel[T])($: Observable[T] => Listanable[T]): Unit = {
        for (item <- $(obs); if item != selection.getSelectedItem) selection.select(item)
        selection.selectedItemProperty().addListener(new ChangeListener[T] {
            override def changed(observable: ObservableValue[_ <: T], oldValue: T, newValue: T): Unit = if (newValue != null) obs << newValue
        })
    }

    def linkBidirPropObservation[O, P](obs: Subject[O], ctrlProp: Property[P])($: Observable[O] => Listanable[O])(implicit p2o: P => O, o2p: O => P): Unit = {
        for (v <- $(obs); if p2o(ctrlProp.getValue) != v) ctrlProp.setValue(v)
        ctrlProp.addListener((_: P, newValue: P) => obs << newValue)
    }

    def linkBidirActObservation[O, P](obs: Subject[O], ctrlProp: ObservableValue[P], apply: O => Unit)($: Observable[O] => Listanable[O])(implicit p2o: P => O): Unit = {
        for (value <- $(obs); if p2o(ctrlProp.getValue) != value) apply(value)
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

        def dispose(): Unit
    }

    abstract class ObservingRenderer[C <: Node] extends Renderer[C] {
        private val resourcesStore = new DisposeStore()

        def $[T](obs: Observable[T]): Listanable[T] = resourcesStore(obs)

        def dispose(): Unit = resourcesStore.dispose()

    }

    class UiSeparatorRenderer(w: UiSeparator) extends ObservingRenderer[Separator] {
        override def render(): Separator = {
            val c = new Separator()
            c.setOrientation(if (w.orientation == UiHoriz) FxOrientation.HORIZONTAL else FxOrientation.VERTICAL)
            c
        }
    }

    class UiLabelRenderer(w: UiLabel) extends ObservingRenderer[Label] {
        override def render(): Label = {
            val c = new Label()
            for (text <- $(w.text)) c.setText(text)
            c
        }
    }

    class UiTextRenderer(w: UiText) extends ObservingRenderer[TextInputControl] {
        override def render(): TextInputControl = {
            val c: TextInputControl = if (w.multi) new TextArea() else new TextField()
            for (obs <- w.text) linkBidirPropObservation(obs, c.textProperty())($)
            for (obs <- w.selection) linkBidirActObservation(obs, c.selectionProperty(), (x: (Int, Int)) => c.selectRange(x._1, x._2))($)
            for (editable <- $(w.editable)) c.setEditable(editable)
            for (disabled <- $(w.disabled)) c.setDisable(disabled)
            c
        }
    }

    class UiStyledTextRenderer(w: UiStyledText) extends ObservingRenderer[TextInputControl] {
        override def render(): TextInputControl = {
            val c: TextInputControl = if (w.multi) new TextArea() else new TextField()
            for (obs <- w.text) linkBidirPropObservation(obs, c.textProperty())($)
            for (obs <- w.selection) linkBidirActObservation(obs, c.selectionProperty(), (x: (Int, Int)) => c.selectRange(x._1, x._2))($)
            for (editable <- $(w.editable)) c.setEditable(editable)
            for (disabled <- $(w.disabled)) c.setDisable(disabled)
            c
        }
    }

    class UiListRenderer[T](list: UiList[T]) extends ObservingRenderer[ListView[T]] {
        private val $row$ = new DisposeStore()
        private val $menu$ = new DisposeStore()

        override def render(): ListView[T] = {
            val c = new ListView[T]()
            for (disabled <- $(list.disabled)) c.setDisable(disabled)
            for (multi <- $(list.multiSelect)) c.getSelectionModel.setSelectionMode(if (multi) SelectionMode.MULTIPLE else SelectionMode.MULTIPLE)
            c.setCellFactory((param: ListView[T]) => {
                val c = new ListCell[T]()
                new ListCell[T] {
                    override def updateItem(item: T, empty: Boolean): Unit = {
                        super.updateItem(item, empty)
                        if (!empty && item != null) {
                            for (text <- $row$(list.valueProvider(item))) setText(text) /// TODO INCORRECT YET, MUST DISPOSE ONE ROW
                        } else setText(null)
                    }
                }
            })
            for (items <- $(list.items)) {
                $row$.dispose()
                c.getItems.clear()
                c.getItems.addAll(items.asJava)
            }
            for (obs <- list.selection) linkBidirMultiSelection(obs, c.getSelectionModel)($)
            for (handler <- list.onDblClick)
                c.setOnMouseClicked((event: MouseEvent) => {
                    if (event.getClickCount == 2 && event.getButton == MouseButton.PRIMARY) {
                        handler(c.getSelectionModel.getSelectedItem)
                    }
                })


            def menuToItems(parent: ContextMenu)(itemModel: UiMenuItem): MenuItem =
                if (itemModel.subitems.isEmpty) applying(new MenuItem(itemModel.text)) { item =>
                    item.setOnAction { evt => parent.hide(); evt.consume(); itemModel.onSelect(); }
                    for (disabled <- $menu$(itemModel.disabled)) item.setDisable(disabled)
                } else applying(new Menu()) { item =>
                    item.setText(itemModel.text)
                    item.getItems.addAll(itemModel.subitems.map(menuToItems(parent)).asJavaCollection)
                    for (disabled <- $menu$(itemModel.disabled)) item.setDisable(disabled)
                }

            for (menuItems: Seq[UiMenuItem] <- $(list.menu); if menuItems.nonEmpty) {
                $menu$.dispose()
                val ctxMenuRoot = applying(new ContextMenu()) { cm =>
                    cm.getItems.addAll(menuItems.map(menuToItems(cm)).asJavaCollection)
                }
                c.setContextMenu(ctxMenuRoot)
            }
            c
        }

        override def dispose(): Unit = {
            $row$.dispose()
            $menu$.dispose()
            super.dispose()
        }
    }

    class UiComboRenderer(combo: UiCombo) extends ObservingRenderer[ComboBox[String]] {
        override def render(): ComboBox[String] = {
            val c = new ComboBox[String]()
            for (disabled <- $(combo.disabled)) c.setDisable(disabled)
            for (items <- $(combo.items)) {
                val selection = c.getSelectionModel.getSelectedItem
                c.getItems.clear()
                c.getItems.addAll(items.asJava)
                if (items.contains(selection)) c.getSelectionModel.select(selection)
                else c.getEditor.setText("")
            }
            for (editable <- $(combo.editable)) c.setEditable(editable)
            for (obs <- combo.text) linkBidirPropObservation(obs, c.getEditor.textProperty())($)
            for (obs <- combo.selection) linkBidirSingleSelect(obs, c.getSelectionModel)($)
            c
        }
    }

    class UiLinkRenderer(link: UiLink) extends ObservingRenderer[Button] {
        override def render(): Button = {
            val c = new Button()
            for (disabled <- $(link.disabled)) c.setDisable(disabled)
            c.setOnAction((event: ActionEvent) => link.onAction())
            for (text <- $(link.text)) c.setText(text)
            c
        }
    }

    class UiButtonRenderer(button: UiButton) extends ObservingRenderer[Button] {
        override def render(): Button = {
            val c = new Button()
            c.setDefaultButton(button.defaultButton)
            c.setCancelButton(button.cancelButton)
            for (disabled <- $(button.disabled)) c.setDisable(disabled)
            c.setOnAction((event: ActionEvent) => button.onAction())
            for (text <- $(button.text)) c.setText(text)
            c
        }
    }

    class UiTableRenderer[T](tableModel: UiTable[T]) extends ObservingRenderer[TableView[T]] {

        private val rowObservables = mutable.HashMap[T, DisposeStore]()

        override def render(): TableView[T] = {
            val tableView = new TableView[T]()
            for (disabled <- $(tableModel.disabled)) tableView.setDisable(disabled)
            tableView.getSelectionModel.setSelectionMode(SelectionMode.MULTIPLE)

            for (columns <- $(tableModel.columns)) {
                val colsToModels: Seq[(TableColumn[T, String], UiColumn[T])] = columns
                        .map(colModel => applying(new TableColumn[T, String]()) { col =>
                            col.setText(colModel.title)
                            col.setCellValueFactory((param: TableColumn.CellDataFeatures[T, String]) => {
                                applying(new SimpleStringProperty()) { prop =>
                                    val rowModel: T = param.getValue
                                    val $row$ = rowObservables(rowModel)
                                    for (value <- $row$(colModel.value(rowModel))) prop.setValue(value)
                                }
                            })
                            col.setSortable(colModel.onSort.isDefined)
                        } -> colModel)
                tableView.getColumns.clear()
                tableView.getColumns.addAll(colsToModels.map(_._1).asJavaCollection)
                tableView.sortPolicyProperty().set { param: TableView[T] =>
                    for ((sortCol, sortColModel) <- tableView.getSortOrder.asScala.headOption.flatMap(sortCol => colsToModels.find(sortCol == _._1))) {
                        sortColModel.onSort.get(sortCol.getSortType == TableColumn.SortType.ASCENDING)
                    }
                    true
                }
            }

            for (itemsOp <- $(tableModel.items)) itemsOp match {
                case SetList(items) =>
                    tableView.getItems.clear()
                    items.foreach(rowObservables += _ -> new DisposeStore())
                    tableView.getItems.setAll(items.asJavaCollection)
                case AddItems(items) =>
                    items.foreach(rowObservables += _ -> new DisposeStore())
                    tableView.getItems.addAll(items.asJavaCollection)
                case InsertItems(index, items) =>
                    items.foreach(rowObservables += _ -> new DisposeStore())
                    tableView.getItems.addAll(index, items.asJavaCollection)
                case RemoveItems(index, amount) =>
                    tableView.getItems.remove(index, index + amount)
                case RemoveItemObjs(items) =>
                    tableView.getItems.removeAll(items.asJava)
            }

            tableView.getItems.addListener(new ListChangeListener[T] {
                override def onChanged(c: ListChangeListener.Change[_ <: T]): Unit = {
                    while (c.next()) {
                        c.getRemoved.forEach { rowModel =>
                                if (!rowObservables.contains(rowModel)) {
                                    println("ERROR: row does not exists " + rowModel)
                                } else {
                                    rowObservables(rowModel).dispose()
                                    rowObservables -= rowModel
                                }
                        }
                    }
                }
            })

            for (selectionSubj <- tableModel.selection) {
                linkBidirMultiSelection(selectionSubj, tableView.getSelectionModel, (selectedItems: Seq[T]) => {
                    if (selectedItems.size == 1) tableView.scrollTo(selectedItems.head)
                })($)
            }

            for (handler <- tableModel.onDblClick) tableView.setOnMouseClicked((event: MouseEvent) =>
                if (event.getClickCount == 2 && event.getButton == MouseButton.PRIMARY && tableView.getSelectionModel.getSelectedItem != null)
                    handler(tableView.getSelectionModel.getSelectedItem))

            def menuToItems(parent: ContextMenu)(itemModel: UiMenuItem): MenuItem =
                if (itemModel.subitems.isEmpty) applying(new MenuItem(itemModel.text)) { item => item.setOnAction { evt => parent.hide(); evt.consume(); itemModel.onSelect(); } }
                else applying(new Menu(itemModel.text)) { item => item.getItems.addAll(itemModel.subitems.map(menuToItems(parent)).asJavaCollection) }

            for (menuItems: Seq[UiMenuItem] <- $(tableModel.menu); if menuItems.nonEmpty) {
                val ctxMenuRoot = applying(new ContextMenu()) { cm =>
                    cm.getItems.addAll(menuItems.map(menuToItems(cm)).asJavaCollection)
                }
                tableView.setContextMenu(ctxMenuRoot)
            }
            tableView
        }

        override def dispose(): Unit = {
            rowObservables.values.foreach(_.dispose())
            rowObservables.clear()
            super.dispose()
        }
    }

    class UiTreeRenderer[T](treeModel: UiTree[T]) extends ObservingRenderer[TreeTableView[T]] {
        override def render(): TreeTableView[T] = {
            val treeTable = new TreeTableView[T]()
            for (disabled <- $(treeModel.disabled)) treeTable.setDisable(disabled)

            for (columns <- $(treeModel.columns)) {
                treeTable.getColumns.addAll(columns.map(colModel => applying(new TreeTableColumn[T, String]()) { col =>
                    col.setText(colModel.title)
                    col.setCellValueFactory((param: TreeTableColumn.CellDataFeatures[T, String]) => {
                        applying(new SimpleStringProperty()) { prop => for (value <- $(colModel.value(param.getValue.getValue))) prop.setValue(value) } /// TODO INCORRECT YET
                    })
                    col.setSortable(false)
                }).asJavaCollection)
            }

            treeTable.setShowRoot(false)
            val rootItem = new TreeItem[T]()
            treeTable.setRoot(rootItem)

            for (menuItems <- $(treeModel.menu)) // TODO MENU MUST BE CHANGED AS IN TABLE
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

                var loaded = false

                setExpanded(treeModel.expanded(itemModel))

                override def isLeaf: Boolean = !treeModel.hasChildren(itemModel)

                override def getChildren: ObservableList[TreeItem[T]] = {
                    if (!loaded) {
                        for (items <- $(treeModel.subitems(itemModel))) super.getChildren.setAll(items.map(new LazyTreeItem(_)).asJavaCollection) /// TODO INCORRECT YET
                        loaded = true
                    }
                    super.getChildren
                }
            }

            for (items <- $(treeModel.items)) rootItem.getChildren.setAll(items.map(new LazyTreeItem(_)).asJavaCollection)

            val selectionModel = treeTable.getSelectionModel
            for (selectionSubj <- treeModel.selection) {
                selectionModel.getSelectedItems.addListener(new ListChangeListener[TreeItem[T]] {
                    override def onChanged(c: ListChangeListener.Change[_ <: TreeItem[T]]): Unit =
                        selectionSubj << c.getList.asScala.map(_.getValue)
                })
                for (items: Seq[T] <- $(selectionSubj); if items != selectionModel.getSelectedItems.asScala.map(_.getValue)) {
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
                        case ("colspan", spanStr) => data.colspan = number(spanStr, "colspan can not be parsed from value ")
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

    class UiPanelRenderer[T](panel: UiPanel)(implicit renderers: FxRenderers) extends ObservingRenderer[Pane] {
        private val childToRenderer = mutable.HashMap[Node, Renderer[_]]()

        private def disposeChildren(): Unit = childToRenderer.foreach { case (_, renderer) => renderer.dispose() }

        override def render(): Pane = {
            val c = new GridPane()
            //c.setGridLinesVisible(false)
            //c.setBorder(Border.EMPTY)

            for (items <- $(panel.items)) {

                val (paneLayout, itemLayouts) = calculateGridLayoutData(panel.layout.asInstanceOf[Grid].markup, items)
                disposeChildren()
                c.getChildren.clear()
                childToRenderer.clear()
                val paneCItems: Iterable[(UiWidget, Renderer[_ <: Node])] = for (item <- items) yield item -> renderers.renderer(item)
                paneCItems.foreach { case (item, renderer) =>
                    val cItem = renderer.render()
                    val l = itemLayouts(item)
                    GridPane.setFillWidth(cItem, l._2.hfill)
                    GridPane.setFillHeight(cItem, l._2.vfill)
                    if (l._2.hgrow) {
                        GridPane.setHgrow(cItem, Priority.SOMETIMES)
                        cItem match {case c: Control => c.setMaxWidth(Double.MaxValue); case _ =>}
                    }
                    if (l._2.vgrow) {
                        GridPane.setVgrow(cItem, Priority.SOMETIMES)
                        cItem match {case c: Control => c.setMaxHeight(Double.MaxValue); case _ =>}
                    }

                    GridPane.setHalignment(cItem, l._2.halign)
                    GridPane.setValignment(cItem, l._2.valign)
                    GridPane.setMargin(cItem, new Insets(paneLayout.margin, paneLayout.margin, paneLayout.margin, paneLayout.margin))

                    c.add(cItem, l._1.x, l._1.y, l._2.colspan, 1)
                    childToRenderer += cItem -> renderer
                }
            }

            c
        }

        override def dispose(): Unit = {
            disposeChildren()
            childToRenderer.clear()
            super.dispose()
        }
    }

    class UiTabPanelRenderer(tabPanelModel: UiTabPanel)(implicit renderers: FxRenderers) extends ObservingRenderer[Node] {
        private val tabToRenderer = mutable.HashMap[Tab, Renderer[_]]()

        override def render(): Node = {
            val pane = new TabPane()
            for (tabs <- $(tabPanelModel.tabs)) {
                tabToRenderer.foreach { case (_, renderer) => renderer.dispose() }
                tabToRenderer.clear()
                pane.getTabs.clear()

                def addTab(tabModel: UiTab, delayContent: Boolean): Unit = {
                    val tab = new Tab()
                    for (label <- $(tabModel.label)) tab.setText(label)
                    for (content <- $(tabModel.content)) {
                        if (tabToRenderer.contains(tab)) tabToRenderer(tab).dispose()
                        val tabRenderer = renderers.renderer(content)

                        if (delayContent)
                            new Thread(() => {
                                Thread.sleep(100L)
                                Platform.runLater(() => tab.setContent(tabRenderer.render()))
                            }).start()
                        else tab.setContent(tabRenderer.render())

                        tabToRenderer += tab -> tabRenderer
                    }
                    tab.setClosable(false)
                    pane.getTabs.add(tab)
                }

                if (tabs.nonEmpty) {
                    addTab(tabs.head, delayContent = false)
                    tabs.tail.foreach(tab => addTab(tab, delayContent = true))
                }
            }
            pane
        }

        override def dispose(): Unit = {
            tabToRenderer.values.foreach(_.dispose())
            tabToRenderer.clear()
            super.dispose()
        }
    }

    class UiTabPanelListOpsRenderer[T](tabPanelModel: UiTabPanelExt[T])(implicit renderers: FxRenderers) extends ObservingRenderer[Node] {

        case class TabData(item: T, var renderer: Renderer[_ <: Node] = null, $: DisposeStore = new DisposeStore(), var moving: Boolean = false)

        implicit class TabDataAccess(tab: Tab) {def data: TabData = tab.getUserData.asInstanceOf[TabData]}

        private val pane = new TabPane()

        override def render(): Node = {
            pane.setTabClosingPolicy(TabClosingPolicy.ALL_TABS)
            val tabModelToTabs: Seq[T] => java.util.Collection[Tab] = _.map { itemModel =>
                val tabModel = tabPanelModel.tab(itemModel)
                val tab = new Tab()
                tab.setUserData(TabData(itemModel))
                val tabLabel = new Label("", tab.getGraphic)
                tab.setGraphic(tabLabel)
                for (label <- tab.data.$(tabModel.label)) tabLabel.setText(label)
                for (content <- tab.data.$(tabModel.content)) {
                    Option(tab.data.renderer).foreach(_.dispose())
                    tab.data.renderer = renderers.renderer(content)
                    tab.setContent(tab.data.renderer.render())
                }
                tab.setClosable(tabPanelModel.closeable)
                tab.setOnClosed { _ => tabPanelModel.onClose(itemModel) }
                tab
            }.asJavaCollection
            for (itemsOp <- $(tabPanelModel.tabs)) itemsOp match {
                case SetList(itemModels) => pane.getTabs.setAll(tabModelToTabs(itemModels))
                case AddItems(itemModels) => pane.getTabs.addAll(tabModelToTabs(itemModels))
                case InsertItems(index, itemModels) => pane.getTabs.addAll(index, tabModelToTabs(itemModels))
                case RemoveItems(index, amount) => pane.getTabs.remove(index, index + amount)
                case RemoveItemObjs(itemModels) => pane.getTabs.removeAll(pane.getTabs.asScala.filter(tab => itemModels.contains(tab.data.item)).asJava)
            }
            pane.getTabs.addListener(new ListChangeListener[Tab] {
                override def onChanged(c: ListChangeListener.Change[_ <: Tab]): Unit =
                    while (c.next())
                        if (c.wasRemoved()) c.getRemoved.asScala
                                .filterNot(_.data.moving)
                                .foreach { tab => tab.data.renderer.dispose(); tab.data.$.dispose() }
            })
            for (selSubj <- tabPanelModel.selection) {
                for (selectedItem <- $(selSubj))
                    pane.getSelectionModel.select(
                        pane.getTabs.asScala.find(tab => tab.data.item == selectedItem).get
                    )
                pane.getSelectionModel.selectedItemProperty().addListener(new ChangeListener[Tab] {
                    override def changed(observable: ObservableValue[_ <: Tab], oldValue: Tab, newValue: Tab): Unit =
                        if (newValue != null)    selSubj << newValue.data.item
                })
            }
            val moveTab = (tab: Tab, pos: Int) => {
                tab.data.moving = true
                pane.getTabs.remove(tab)
                pane.getTabs.add(pos, tab)
                pane.getSelectionModel.select(tab)
                tab.data.moving = false
                tabPanelModel onTabsOrdered pane.getTabs.asScala.map(tab => tab.data.item).zipWithIndex
            }
            DraggableTabs.decorate(pane, moveTab)
            pane
        }

        override def dispose(): Unit = {
            pane.getTabs.forEach { tab => tab.data.renderer.dispose(); tab.data.$.dispose() }
            super.dispose()
        }
    }

    object DraggableTabs {

        def decorate[T](tabPane: TabPane, moveTab: (Tab, Int) => Any): Unit = {
            val decorId = UUID.randomUUID().toString
            var currentDraggingTab: Tab = null

            def decorateTab(tab: Tab): Unit = {
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
                        moveTab(currentDraggingTab, idx)
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

        private var elsRenderers = mutable.ArrayBuffer[Renderer[_ <: Node]]()

        override def render(): Node = {
            val pane = new SplitPane()
            pane.setOrientation(if (paneModel.orientation == UiHoriz) FxOrientation.HORIZONTAL else FxOrientation.VERTICAL)
            elsRenderers += renderers.renderer(paneModel.els._1)
            elsRenderers += renderers.renderer(paneModel.els._2)

            pane.getItems.addAll(elsRenderers.map(_.render()).asJava)
            pane.setDividerPosition(0, paneModel.proportion.toDouble / 100)
            pane
        }

        override def dispose(): Unit = elsRenderers.foreach(_.dispose())
    }

    class UiComponentRenderer(comp: UiComponent)(implicit renderers: FxRenderers) extends Renderer[Node] {

        private var renderer: Renderer[_ <: Node] = _

        override def render(): Node = {
            val widget = comp.content()
            renderer = renderers.renderer(widget)
            renderer.render()
        }

        override def dispose(): Unit = renderer.dispose()
    }

    class UiObservingComponentRenderer(comp: UiObservingComponent)(implicit renderers: FxRenderers) extends UiComponentRenderer(comp) {

        override def dispose(): Unit = {comp.dispose(); super.dispose() }
    }

    trait FxRenderers extends UiRenderer {

        def renderer(w: UiWidget)(implicit renderers: FxRenderers): Renderer[_ <: Node]

        def uiScheduler(): Scheduler = () => new Worker {
            private var disposed = false

            override def schedule(run: Runnable, delay: Long, unit: TimeUnit): Disposable = {/*println("Do async"); */ Platform.runLater(run); this }

            override def dispose(): Unit = disposed = true

            override def isDisposed: Boolean = disposed
        }

        override def runApp(root: UiWidget, postAction: UiRenderer => Unit = null): Unit = {
            FxRender.rootContent = root
            FxRender.postAction = postAction
            //LauncherImpl.launchApplication(classOf[App], Array())
            FxApp.appRunner = (primaryStage: Stage) => startApp(primaryStage)
            FxApp.main(Array())
        }

        def startApp(primaryStage: Stage): Unit = {
            FxRender.primaryStage = primaryStage
            val rootContent = FxRender.renderers.renderer(FxRender.rootContent)(FxRender.renderers).render()
            applyContentToState(rootContent, primaryStage, fullScreen = true)
            primaryStage.show()
            if (postAction != null) postAction(FxRender.renderers)
        }

        override def runModal(content: UiWidget, hideTitle: Boolean = false, close: Option[Subject[_ >: Unit]] = None): Unit = {
            val $ = new DisposeStore()
            val renderer = FxRender.renderers.renderer(content)(FxRender.renderers)
            val dialog = new Stage(if (hideTitle) StageStyle.UNDECORATED else StageStyle.DECORATED)

            applyContentToState(renderer.render(), dialog, fullScreen = false, panelBorder = hideTitle)

            for (obs <- close; _ <- $(obs)) {
                dialog.close()
            }

            dialog.initOwner(FxRender.primaryStage)
            dialog.initModality(Modality.WINDOW_MODAL)
            dialog.showAndWait()
            for (x <- close) x << {}
            renderer.dispose()
            $.dispose()
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
            case x: UiSeparator => new UiSeparatorRenderer(x)
            case x: UiObservingComponent => new UiObservingComponentRenderer(x)
            case x: UiComponent => new UiComponentRenderer(x)
        }

        override def runApp(root: UiWidget, postAction: UiRenderer => Unit = null): Unit = {
            FxRender.renderers = this
            super.runApp(root, postAction)
        }

    }

    def grabFullContent(node: Node)(implicit renderers: FxRenderers): Pane = {
        val pane = new GridPane()
        val contentNode = node
        GridPane.setHgrow(contentNode, Priority.ALWAYS)
        GridPane.setVgrow(contentNode, Priority.ALWAYS)
        GridPane.setFillWidth(contentNode, true)
        GridPane.setFillHeight(contentNode, true)
        pane.add(contentNode, 0, 0)
        pane
    }

    def applyContentToState(content: Node, stage: Stage, fullScreen: Boolean, panelBorder: Boolean = false): Unit = {
        val rootPane = grabFullContent(content)
        if (panelBorder)
            rootPane.setBorder(new Border(new BorderStroke(Color.BLACK, BorderStrokeStyle.SOLID, CornerRadii.EMPTY, BorderWidths.DEFAULT)))
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
    var postAction: UiRenderer => Unit = _

}
