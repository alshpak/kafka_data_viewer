package devtools.kafka_data_viewer.ui

import java.util.Properties

import scala.xml.XML

trait AppUpdateState

case class AppUpdateUnavailable(reason: String) extends AppUpdateState

case class AppUpdateCurrent(version: String) extends AppUpdateState

case class AppUpdateRequired(version: String, newVersion: String) extends AppUpdateState



object KafkaToolUpdate {

    type UpdateUrl = String

    def retrieveUpdateState(): (AppUpdateState, UpdateUrl) =
        (for {
            propsStream <- Option(KafkaToolUpdate.getClass.getClassLoader.getResourceAsStream("build.properties"))
            props = {val p = new Properties(); p.load(propsStream); p }
            version = props.getProperty("version")
            baseUrl = props.getProperty("url")
            metaUrl = baseUrl + "/maven-metadata.xml"
        } yield {
            (try {
                val versioning = XML.load(metaUrl) \\ "versioning"
                val versions = (versioning \ "versions" \ "version").map(_.text)
                val release = (versioning \ "release").text
                val latest = (versioning \ "latest").text
                println("Url " + baseUrl + " current " + version + " and new is " + release)
                if (latest == version) AppUpdateCurrent(version)
                else AppUpdateRequired(version, latest)
            } catch {
                case e: Exception => e.printStackTrace(); AppUpdateUnavailable(e.getMessage)
            }, baseUrl)
        }).getOrElse((AppUpdateUnavailable("no build info found"), null))


    def test(): Unit = {

        def t[T <: {def m() : String}](t: T): String = t.m()

        class M {
            def m(): String = "Hello"
        }

        class M2 {
            def m(): Int = 2
        }

        assert(t(new M()) == "Hello")

        val a = Seq("a", "b", "cc")
        val charNums: Seq[Int] = a.flatMap(el => el.toCharArray).map(c => c.toInt)

        val charNums2: Seq[Int] = for {
            s <- a
            ch <- s.toCharArray
            n = ch.toInt
        } yield n

        //a.sum
        charNums.sum

    }
}
