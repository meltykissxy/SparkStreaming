import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object RDDStream {

    def main(args: Array[String]) {

        //1.初始化Spark配置信息
        val conf = new SparkConf().setMaster("local[*]").setAppName("RDDStream")

        //2.初始化SparkStreamingContext
        val ssc = new StreamingContext(conf, Seconds(4))

        //3.创建RDD队列
        val rddQueue = new mutable.Queue[RDD[Int]]()

        //4.创建QueueInputDStream
        val inputStream = ssc.queueStream(rddQueue,oneAtATime = false)

        //5.处理队列中的RDD数据
        val mappedStream = inputStream.map((_,1))
        val reducedStream = mappedStream.reduceByKey(_ + _)

        //6.打印结果
        reducedStream.print()

        //7.启动任务
        ssc.start()

        //8.循环创建并向RDD队列中放入RDD。注意看这里的位置，在start后面
        for (i <- 1 to 5) {
            rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
            Thread.sleep(2000)
        }

        ssc.awaitTermination()

    }
}

