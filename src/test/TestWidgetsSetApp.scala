import devtools.lib.rxext.ListChangeOps.SetList
import devtools.lib.rxext.Observable
import devtools.lib.rxext.Observable.just
import devtools.lib.rxext.Subject.{behaviorSubject, publishSubject}
import devtools.lib.rxui.FxRender.DefaultFxRenderes
import devtools.lib.rxui.UiImplicits._
import devtools.lib.rxui.{UiLabel, _}

object TestWidgetsSetApp {

    class TestUi(val layoutData: String = "") extends UiComponent {

        private val text = publishSubject[String]()
        private val textSelection = publishSubject[(Int, Int)]()

        private val text2 = publishSubject[String]()

        private val listAndComboItems = just(Seq("One elem", "Two elem", "Three elem"))
        private val compoText = publishSubject[String]()
        private val comboSelection = publishSubject[String]()
        private val listSelection = publishSubject[Seq[String]]()
        private val listOnDblClick = publishSubject[String]()

        type TableRow = (String, String, Double)
        private val tableItems = behaviorSubject[Seq[TableRow]](Seq(
            ("row1", "data1", 2.0),
            ("row2", "data2", 3.0)
        ))
        private val tableSelection = publishSubject[Seq[TableRow]]()
        private val tableDblClickSelection = publishSubject[TableRow]()

        case class TreeRow(label: String, subitems: Seq[TreeRow] = Seq())

        private val treeItems = behaviorSubject[Seq[TreeRow]](Seq(
            TreeRow("Row 1", subitems = Seq(
                TreeRow("Row 1.1", subitems = Seq(
                    TreeRow("Row 1.1.1")
                )),
                TreeRow("Row 1.2")
            )),
            TreeRow("Row 2")
        ))
        private val treeSelection = publishSubject[Seq[TreeRow]]()
        private val treeDblClickSelection = publishSubject[TreeRow]()

        case class TabContent(label: String)

        private val tabItems = behaviorSubject[Seq[TabContent]](Seq(
            TabContent("one"),
            TabContent("two"),
            TabContent("three"),
            TabContent("four"),
            TabContent("five")
        ))

        override def content(): UiWidget = content1()

        def content0(): UiWidget = UiPanel("", Grid("cols 2"), items = Seq(
            UiLabel(text = "Text"),
            UiText("grow", text = text, selection = textSelection, multi = true),

            UiLabel(text = "Selection"),
            UiLabel(text = textSelection.map(fromTo => "From " + fromTo._1 + " to " + fromTo._2)),

            UiLabel(text = just("next label")),
            UiText(text = text)
        ))

        def content1(): UiWidget = UiPanel("", Grid(), items = Seq(
            UiTabPanel("grow", tabs = Seq(
                UiTab(label = "Text references", content = UiPanel("", Grid("cols 2"), items = Seq(
                    UiLabel(text = "Text"),
                    UiText("grow", text = text, selection = textSelection, multi = true),

                    UiLabel(text = "Selection"),
                    UiLabel(text = textSelection.map(fromTo => "From " + fromTo._1 + " to " + fromTo._2)),

                    UiLabel(text = just("next label")),
                    UiText(text = text)
                ))),
                UiTab(label = "List And Combo Test", content = UiPanel("", Grid(), items = Seq(
                    UiCombo(items = listAndComboItems, text = compoText, selection = comboSelection),
                    UiLabel(text = compoText.map("Combo text is " + _)),
                    UiLabel(text = comboSelection.map("Combo selection is " + _)),
                    UiList[String]("grow",
                        items = listAndComboItems,
                        selection = listSelection,
                        onDblClick = listOnDblClick),
                    UiLabel(text = listSelection.map(_.headOption.map("First selection is " + _).getOrElse("No selection"))),
                    UiLabel(text = listOnDblClick.map("On Double Click is " + _))
                ))),
                UiTab(label = "Table", content = UiPanel("", Grid(), items = Seq(
                    UiTable[TableRow]("grow",
                        items = tableItems.map(SetList(_)),
                        selection = tableSelection,
                        onDblClick = tableDblClickSelection,
                        columns = Seq(
                            UiColumn(title = "Col1", value = (row: TableRow) => row._1)
                        )
                    ),
                    UiLabel(text = tableSelection.map("Selected row is " + _)),
                    UiLabel(text = tableDblClickSelection.map("Selected dbl click row is " + _))
                ))),
                UiTab(label = "Tree", content = UiPanel("", Grid(), items = Seq(
                    UiTree[TreeRow]("grow",
                        items = treeItems,
                        selection = treeSelection,
                        onDblClick = treeDblClickSelection,
                        subitems = _.subitems,
                        hasChildren = _.subitems.nonEmpty,
                        menu = Seq(UiMenuItem(text = "Menu 1", onSelect = () => Unit)),
                        columns = Seq[UiColumn[TreeRow]](
                            UiColumn(title = "Col 1", value = (x: TreeRow) => x.label)
                        ),
                    ),
                    UiLabel(text = treeSelection.map("Selected row is " + _)),
                    UiLabel(text = treeDblClickSelection.map("Selected dbl click row is " + _))
                ))),
                UiTab(label = "Draggable tabs", content = UiPanel("", Grid(), items = Seq(
                    UiTabPanelExt[TabContent]("grow",
                        tabs = tabItems.map(SetList(_)),
                        tab = tabContent => UiTab(label = tabContent.label, content = UiLabel(text = tabContent.label)),
                        closeable = true
                    )
                ))),
                UiTab(label = "Updatable table", content = UiPanel("", Grid(), items = Seq(
                    new UiObservingComponent() {
                        override val layoutData: String = "grow"

                        class Item(val title: String) {
                            private val valueSubject = behaviorSubject("")

                            def change(): Unit = valueSubject << (valueSubject.value + "updated")

                            def value: Observable[String] = valueSubject
                        }

                        private val items: Seq[Item] = Seq(
                            new Item("item 1"),
                            new Item("item 2")
                        )

                        private val selection = publishSubject[Seq[Item]]()
                        private val onItemsUpdate = publishSubject[Unit]()
                        for ((_, selection) <- $(onItemsUpdate.withLatestFrom(selection)))
                            for (item <- selection) item.change()

                        override def content(): UiWidget =
                            UiTable[Item](
                                items = just(SetList(items)),
                                columns = Seq[UiColumn[Item]](
                                    UiColumn(title = "Key", _.title),
                                    UiColumn(title = "Value", _.value)
                                ),
                                selection = selection,
                                menu = selection.map(selItems =>
                                    if (selItems.size == 1)
                                        Seq[UiMenuItem](UiMenuItem(text = "Do Update for item", onSelect = onItemsUpdate))
                                    else
                                        Seq[UiMenuItem](UiMenuItem(text = "Do update all items", subitems = Seq(
                                            UiMenuItem(text = "Do update for all", onSelect = onItemsUpdate),
                                            UiMenuItem(text = "Do update for nothing")
                                        )))
                                )
                            )
                    }

                ))),
                UiTab(label = "Lazy Tree", content = UiPanel("", Grid(), items = Seq(
                    new UiComponent {
                        override val layoutData: String = "grow"

                        trait TreeItem {
                            val label: String
                            val value: String = ""

                            def query(): Seq[TreeItem] = Seq()

                            private lazy val items = behaviorSubject[Seq[TreeItem]](query())

                            def subitems(): Observable[Seq[TreeItem]] = items

                        }

                        case class RootItem(label: String = "Root") extends TreeItem {
                            override def query(): Seq[TreeItem] = {
                                Thread.sleep(2000)
                                Seq(SubItem())
                            }
                        }

                        case class SubItem(label: String = "SubItem", override val value: String = "SomeValue") extends TreeItem {
                            override def query(): Seq[TreeItem] = Seq(LeafItem())
                        }


                        case class LeafItem(label: String = "LeafItem", override val value: String = "Leaf Value") extends TreeItem

                        override def content(): UiWidget =
                            UiTree[TreeItem](layoutData,
                                items = Seq(RootItem()),
                                columns = Seq[UiColumn[TreeItem]](
                                    UiColumn("Label", _.label),
                                    UiColumn("Value", _.value)
                                ),
                                expanded = _ => false,
                                subitems = _.subitems(),
                                hasChildren = {
                                    case _: RootItem => true
                                    case _: SubItem => true
                                    case _ => false
                                }
                            )


                    }
                )))
            ))
        ))
    }

    def main(args: Array[String]): Unit = {
        DefaultFxRenderes.runApp(new TestUi(""))
    }
}
