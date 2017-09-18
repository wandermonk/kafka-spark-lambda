import Domain.Activity
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.HasOffsetRanges

package object Functions {
  def rddToRDDActivity(input: RDD[(String, String)]) = {
    val offsetRanges = input.asInstanceOf[HasOffsetRanges].offsetRanges
    input.mapPartitionsWithIndex({(index,iterator) =>
      val or = offsetRanges(index)
      iterator.flatMap { kv =>
          val line = kv._2
          val record = line.split("\\t")
          val MS_IN_HOUR = 1000 * 60 * 60
          if (record.length == 7)
            Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6),
              Map("topic" -> or.topic,"kafkapartition" -> or.partition.toString,"fromoffset"->or.fromOffset.toString,"untiloffset"->or.untilOffset.toString)))
          else
            None
      }
    })
  }
}
