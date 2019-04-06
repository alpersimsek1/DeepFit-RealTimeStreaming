import java.util
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.{Bucket, CouchbaseCluster}
import rx.Observable
import rx.functions.Func1

object CouchbaseManager extends Serializable {
  val cluster: CouchbaseCluster = CouchbaseCluster
    .create("")
    .authenticate("", "")
  val bucket: Bucket = cluster.openBucket("ProductContentScore")
  bucket.bucketManager.createN1qlPrimaryIndex(true, false)

  def bulkGet(ids: util.Collection[String], bucket: Bucket): util.List[JsonDocument] = Observable
    .from(ids)
    .flatMap {
      new Func1[String, Observable[JsonDocument]]() {
        override def call(id: String): Observable[JsonDocument] = {
          bucket.async.get(id)
        }
      }
    }
    .toList
    .toBlocking
    .single
}

