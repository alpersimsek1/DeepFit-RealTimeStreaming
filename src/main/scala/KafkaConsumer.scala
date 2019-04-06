import java.util.Properties

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.util.serialization.{JSONKeyValueDeserializationSchema, KeyedDeserializationSchema, KeyedDeserializationSchemaWrapper}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.flink.api.java.typeutils.TypeExtractor.getForClass

object KafkaConsumer extends Serializable {

  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "kafka:9092")
  properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000")
  properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10000")
  properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
  properties.setProperty("group.id", "test")

  def main(args: Array[String]): Unit = {
    val consumer = new FlinkKafkaConsumer010("test", new Serialize(), properties)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataset = env.addSource(consumer)
    val reducedKeys = dataset
      .map {
        row =>
          (row._1, row._2.toDouble, 1.0)
      }
      .keyBy(0)
      .timeWindow(Time.minutes(1))
      .reduce((v1, v2) => (v1._1, v1._2 + v2._2, v1._3 + v2._3))
    reducedKeys.addSink(new CouchbaseSink())
    env.execute()
  }

  private class Serialize extends KeyedDeserializationSchema[(String, String)] {
    private val mapper = new ObjectMapper()
    override def deserialize(messageKey: Array[Byte], message: Array[Byte], topic: String,
                             partition: Int, offset: Long): (String, String) = {
      val value = message.map(_.toChar).mkString
      val key = messageKey.map(_.toChar).mkString
      (key, value)
    }
    override def isEndOfStream(nextElement: (String, String)): Boolean = false
    override def getProducedType: TypeInformation[(String, String)] = getForClass(classOf[(String, String)])

  }

}
