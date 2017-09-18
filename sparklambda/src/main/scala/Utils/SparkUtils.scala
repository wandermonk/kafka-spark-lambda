package Utils

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkUtils {
  def getSparkContext(appName: String): SparkContext = {
    val checkPointDirectory = "hdfs://quickstart.cloudera:8020/spark/checkpoint"
    val conf = new SparkConf().setAppName(appName)
      .set("spark.cassandra.conection.host","localhost")
    val sc = SparkContext.getOrCreate(conf)
    sc.setCheckpointDir(checkPointDirectory)
    sc
  }

  def getSQLContext(sc: SparkContext) = {
    val sqlContext = SQLContext.getOrCreate(sc)
    sqlContext
  }

  def getStreamingContext(StreamingApp: (SparkContext, Duration) => StreamingContext, sc: SparkContext, batchDuration: Duration) = {
    val creatingFunc = () => StreamingApp(sc, batchDuration)
    val ssc = sc.getCheckpointDir match {
      case Some(checkPointDir) => StreamingContext.getActiveOrCreate(checkPointDir, creatingFunc, sc.hadoopConfiguration, createOnError = true)
      case None => StreamingContext.getActiveOrCreate(creatingFunc)
    }
    sc.getCheckpointDir.foreach(cp => ssc.checkpoint(cp))
    ssc
  }
}
