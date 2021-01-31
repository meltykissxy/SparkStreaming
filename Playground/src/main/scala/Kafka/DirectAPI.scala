package Kafka

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectAPI {

    def main(args: Array[String]): Unit = {

        //1.创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setAppName("ReceiverWordCount").setMaster("local[*]")

        //2.创建StreamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        //3.定义Kafka参数
        val kafkaPara: Map[String, Object] = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
            ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
            "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
        )

        //4.读取Kafka数据创建DStream
        // 这里的KafkaUtils是Spark Streaming自带的
        // ConsumerRecord[String, String]，Kafka的数据是K-V键值对形式，K默认为空
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
            LocationStrategies.PreferConsistent,// 当前的节点跟Kafka的服务器放在什么位置，最好在一块儿

            ConsumerStrategies.Subscribe[String, String](Set("atguigu"), kafkaPara))

        //5.将每条消息的KV取出
        val valueDStream: DStream[String] = kafkaDStream.map(record => record.value())

        //6.计算WordCount
        valueDStream.flatMap(_.split(" "))
            .map((_, 1))
            .reduceByKey(_ + _)
            .print()

        //7.开启任务
        ssc.start()
        ssc.awaitTermination()
    }
}
