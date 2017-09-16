package BatchJob

import Domain._
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object BatchJob {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark Streaming")
    val sc = new SparkContext(conf)
    implicit val sqlContext = new SQLContext(sc)

    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    val sourceFile = "filepath"
    val input = sc.textFile(sourceFile)

    input.foreach(println)

    val inputDF = input.flatMap { line =>
      val record = line.split("\\t")
      val MS_IN_HOUR = 1000 * 60 * 60
      if (record.length == 7)
        Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6)))
      else
        None

    }.toDF()

    val df = inputDF.select(
      add_months(from_unixtime(inputDF("timestamp_hour")/1000),1).as("timestamp_hour"),
      inputDF("referrer"),inputDF("action"),inputDF("prevPage"),inputDF("page"),inputDF("visitor"),inputDF("product"),inputDF("inputProps")
    ).cache()

    df.registerTempTable("activity")

    val visitorsByProduct = sqlContext.sql(
      """select product,timestamp_hour,count(distinct visitor)
        |from activity group by product,timestamp_hour
        |as unique_visitors """.stripMargin)

    val activityByProduct = sqlContext.sql(
      """
        |select product,timestamp_hour,
        |sum(case when action ='purchase' then 1 else 0 end) as purchase_count,
        |sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
        |sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
        |from activity group by product,timestamp_hour
      """).cache()

    activityByProduct.write.partitionBy("timestamp_hour").mode(SaveMode.Append).parquet("hdfs://quickstart.cloudera:8020/lambda/batch1")

    df.registerTempTable("activityByProduct")
    visitorsByProduct.foreach(println)
    activityByProduct.foreach(println)
  }
}
