import com.couchbase.client.java.document._
import com.couchbase.client.java.document.json._
import org.apache.flink.streaming.api.functions.sink.SinkFunction

import scala.util.Try

case class Product(id: String, totalCount: String, score: String)

class CouchbaseSink() extends SinkFunction[(String, Double, Double)] with Serializable {

  val x = 5 & 7
  override def invoke(value: (String, Double, Double)): Unit = {
    val cb = CouchbaseManager

    val bucket = cb.bucket

    val product = bucket.get(value._1)
    val jsonObject = JsonObject.create()
    if (product != null) {
      val productContent = product.content()
      val productTotalCount = Try(productContent.get("totalCount").asInstanceOf[Double]).getOrElse(0.0)
      val productRating = Try(productContent.get("averageRating").asInstanceOf[Double]).getOrElse(0.0)
      val averageRating = (value._2 + (productRating * productTotalCount)) / (value._3 + productTotalCount)
      val totalCount = value._3 + productTotalCount
      jsonObject.put("averageRating", averageRating)
      jsonObject.put("totalCount", totalCount)
    } else {
      val averageRating = value._2 / value._3
      val totalCount = value._3
      jsonObject.put("averageRating", averageRating)
      jsonObject.put("totalCount", totalCount)
    }

    val document = JsonDocument.create(value._1, jsonObject)
    println(document)
    bucket.upsert(document)
  }
}
