import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount02 {
    def main(args: Array[String]) {
        // 定义更新状态方法，参数values为当前批次单词频度，state为以往批次单词频度
        // 例如上一个rdd有8个chj，那么就传过来(1, 1, 1, 1, 1, 1, 1, 1)而不是(8)
        val updateFunc = (values: Seq[Int], state: Option[Int]) => {
            val currentCount = values.foldLeft(0)(_ + _)
            val previousCount = state.getOrElse(0)
            Some(currentCount + previousCount)
        }

        val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
        val ssc = new StreamingContext(conf, Seconds(3))
        ssc.checkpoint("./ck")

        // Create a DStream that will connect to hostname:port, like hadoop102:9999
        val lines = ssc.socketTextStream("hadoop102", 9999)

        // Split each line into words
        val words = lines.flatMap(_.split(" "))

        //import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
        // Count each word in each batch
        val pairs = words.map(word => (word, 1))

        // 使用updateStateByKey来更新状态，统计从运行开始以来单词总的次数
        val stateDstream = pairs.updateStateByKey[Int](updateFunc)
        stateDstream.print()

        ssc.start()             // Start the computation
        ssc.awaitTermination()  // Wait for the computation to terminate
        //ssc.stop()
    }

}
