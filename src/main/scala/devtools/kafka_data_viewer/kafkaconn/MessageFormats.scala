package devtools.kafka_data_viewer.kafkaconn

import java.io._
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import devtools.kafka_data_viewer.kafkaconn.Connector.withClosable
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.generic.GenericData
import org.apache.avro.reflect.ReflectData
import org.apache.avro.{Conversions, Schema}
import tech.allegro.schema.json2avro.converter.JsonAvroConverter

import scala.language.postfixOps

object MessageFormats {

    trait MessageType {
        def formatter(): MessageFormat

        def display: String
    }

    case object StringMessage extends MessageType {
        override def formatter(): MessageFormat = StringMessageFormat

        override def display: String = "String"
    }

    case object ZipMessage extends MessageType {
        override def formatter(): MessageFormat = GZipMessageFormat

        override def display: String = "GZIP"
    }

    case class AvroMessage(registry: String) extends MessageType {
        override def formatter(): MessageFormat = new AvroMessageFormat(registry)

        override def display: String = "Avro:" + registry
    }

}

trait MessageFormat {

    def encode(topic: String, message: String): Array[Byte]

    def decode(topic: String, bytes: Array[Byte]): String

}

object StringMessageFormat extends MessageFormat {
    override def encode(topic: String, s: String): Array[Byte] = s.getBytes("UTF8")

    override def decode(topic: String, b: Array[Byte]): String = new String(b, "UTF8")
}

object GZipMessageFormat extends MessageFormat {
    override def encode(topic: String, s: String): Array[Byte] = {
        val buf = new ByteArrayOutputStream()
        withClosable(() => new OutputStreamWriter(new GZIPOutputStream(buf)))(_.write(s))
        buf.toByteArray
    }

    override def decode(topic: String, b: Array[Byte]): String = {
        try {
            val isf = () => new BufferedReader(new InputStreamReader(
                new GZIPInputStream(new ByteArrayInputStream(b))))
            val readContent = (br: BufferedReader) => Stream.continually(br.readLine()).takeWhile(null !=).mkString("\n")
            withClosable(isf)(readContent)
        } catch {
            case e: Exception => "Not a GZIP: " + e.toString + "; Message: " + new String(b, "UTF8")
        }
    }
}

class AvroMessageFormat(avroServer: String) extends MessageFormat {

    private val schemaClient = Option(avroServer).map(host => {
        ReflectData.get().addLogicalTypeConversion(new Conversions.DecimalConversion())
        new CachedSchemaRegistryClient(host, 100)
    })
    private val registryDeser = schemaClient.map(client => new KafkaAvroDeserializer(client))


    override def encode(topic: String, message: String): Array[Byte] = {
        val schemaMeta = schemaClient.get.getLatestSchemaMetadata(topic)
        val schema = new Schema.Parser().parse(schemaMeta.getSchema)

        val converter = new JsonAvroConverter()
        val record = converter.convertToGenericDataRecord(message.getBytes, schema)
        val bytes = new KafkaAvroSerializer(schemaClient.get).serialize(topic, record)
        bytes
    }

    override def decode(topic: String, bytes: Array[Byte]): String = {
        try {
            val record = registryDeser.get.deserialize(topic, bytes).asInstanceOf[GenericData.Record]
            val converter = new JsonAvroConverter()
            val json = new String(converter.convertToJson(record), "UTF8")
            json
        } catch {
            case e: Exception =>
                "Can not unpack " + e.toString
        }
    }
}