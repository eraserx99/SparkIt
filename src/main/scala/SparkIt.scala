import org.apache.spark.{SparkConf, SparkContext}

object SparkIt {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("SparkIt")
    val sc = new SparkContext(conf)


  }
}
