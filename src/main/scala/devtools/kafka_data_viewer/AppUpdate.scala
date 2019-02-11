package devtools.kafka_data_viewer

import java.net.{HttpURLConnection, URL}
import java.util.Properties

import com.fasterxml.jackson.databind.ObjectMapper

object AppUpdate {

    case class AppUpdateInfo(currentVersion: String, newVersion: String, updateUrl: String)

    def verify(): Option[AppUpdateInfo] = {


        try {
            (for {
                propsStream <- Option(AppUpdate.getClass.getClassLoader.getResourceAsStream("build.properties"))
                props = {val p = new Properties(); p.load(propsStream); p }
                version = props.getProperty("version")
                baseUrl = props.getProperty("url")
                updateUrl = props.getProperty("update_url")
            } yield {
                val url = new URL(baseUrl)
                val con = url.openConnection()
                val in = con.getInputStream

                val mapper = new ObjectMapper()
                val json = mapper.readValue(in, classOf[java.util.HashMap[_, _]])

                val newVersion = json.get("name").asInstanceOf[String]

                in.close()
                if (version != newVersion)
                    Some(AppUpdateInfo(
                        currentVersion = version,
                        newVersion = newVersion,
                        updateUrl = updateUrl
                    ))
                else
                    None
            }).flatten
        } catch {
            case e: Exception =>
                e.printStackTrace()
                None
        }

    }
}
