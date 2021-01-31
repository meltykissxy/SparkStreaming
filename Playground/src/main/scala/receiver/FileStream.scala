package receiver

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileStream {

    def main(args: Array[String]): Unit = {

        //1.初始化Spark配置信息
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

        //2.初始化SparkStreamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(5))

        //3.创建自定义receiver的Streaming
        val lineStream = ssc.receiverStream(new CustomerReceiver("hadoop102", 9999))

        //4.将每一行数据做切分，形成一个个单词
        val wordStream = lineStream.flatMap(_.split("\t"))

        //5.将单词映射成元组（word,1）
        val wordAndOneStream = wordStream.map((_, 1))

        //6.将相同的单词次数做统计
        val wordAndCountStream = wordAndOneStream.reduceByKey(_ + _)

        //7.打印
        wordAndCountStream.print()

        //8.启动SparkStreamingContext
        ssc.start()
        ssc.awaitTermination()
    }
}

