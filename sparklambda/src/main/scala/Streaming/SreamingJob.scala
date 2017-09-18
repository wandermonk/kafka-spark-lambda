package Streaming

import Utils.SparkUtils._
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.Duration
import Domain.ActivityByProduct
import config.Settings
import org.apache.commons.codec.StringDecoder
import Functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.kafka.KafkaUtils

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._

object SreamingJob {
  def main(args: Array[String]): Unit = {
    //setup spark context
    val sc = getSparkContext("Spark Streaming")
    val destination = Settings.WebLogGen
    val sqlContext = getSQLContext(sc)
    import sqlContext.implicits._

    val batchDuration = Seconds(4)

    def streaminApp(sc: SparkContext, batchDuration: Duration) = {
      //setup spark streaming context
      val ssc = new StreamingContext(sc, batchDuration)
      val wlc = Settings.WebLogGen
      //val inputPath = "hdfs:////quickstart.cloudera:8020//user//data"
       val topic = wlc.kafkaTopic

     // val kafkaParams = Map(
     //   "zookeeper.connect" -> "localhost:2181",
       // "group.id" -> "lambda",
        //"auto.offset.reset" -> "largest"
     // )


      //val kafkaStream = KafkaUtils.createStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Map(topic -> 1),StorageLevel.MEMORY_AND_DISK).map(_._2)

      val kafkaDirectParams = Map(
        "metadata.broker.list" -> "localhost:9092",
        "group.id" -> "lambda",
        "auto.offset.reset" -> "smallest"
      )

      //DirectStream is better than Receiver based approach

      val kafkaDirectStream = KafkaUtils.createDirectStream[String,String,
        StringDecoder,StringDecoder](ssc,kafkaDirectParams,Set(topic))

     // val textDStream = ssc.textFileStream(inputPath)
      val activityStream = kafkaDirectStream.transform(input =>
        Functions.rddToRDDActivity(input)
      ).cache()

      activityStream.transform(rdd => {
        val df = rdd.toDF()
        df.registerTempTable("activity")
        val activityByProduct = sqlContext.sql(
          """
            |select product,timestamp_hour,
            |sum(case when action ='purchase' then 1 else 0 end) as purchase_count,
            |sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
            |sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
            |from activity group by product,timestamp_hour
          """)


        activityStream.foreachRDD(rdd => {
          val activityDF = rdd
            .toDF()
              .selectExpr("timestamp_hour","referrer","action","prevPage","page","visitor"
                ,"product","inputProps.topic as topic","inputProps.kafkapartition as kafkapartition"
                ,"inputProps.fromoffset as fromoffset","inputProps.untiloffset as untiloffset")

          activityDF
            .write
            .partitionBy("topic","kafkapartition","timestamp_hour")
            .mode(SaveMode.Append)
            .parquet(destination.destPath)
        }
        )
        activityByProduct.map(r => ((r.getString(0), r.getLong(1)), ActivityByProduct(r.getString(0), r.getLong(1), r.getLong(2), r.getLong(3), r.getLong(4))
        ))
      }
      ).print()
      ssc
    }

    val ssc = getStreamingContext(streaminApp, sc, batchDuration)
    ssc.start()
    ssc.awaitTermination()
  }
}
