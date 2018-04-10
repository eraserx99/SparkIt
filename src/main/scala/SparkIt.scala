import org.apache.spark.{SparkConf, SparkContext}

object SparkIt {

  def countTotalLength(sc: SparkContext, dir: String) = {
    val lines = sc.textFile(dir)
    val lineLengths = lines.map(l => l.length)

    lineLengths.reduce((a, b) => a + b)
  }

  def splitAtSpace(sc: SparkContext, dir: String, minLength: Int = 0) = {
    val lines = sc.textFile(dir)
    if(minLength > 0)
      lines.flatMap(_.split(" ")).filter(_.length >= minLength)
    else
      lines.flatMap(_.split(" "))
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("SparkIt")
    val sc = new SparkContext(conf)

    val listLength = List("article", "gallery", "person", "video").map((s: String) => countTotalLength(sc, "src/main/resources/" + s + "/*.json"))
    print(List("article", "gallery", "person", "video") zip listLength)

    splitAtSpace(sc, "src/main/resources/article/article_9996786.json").collect().foreach(println)
    splitAtSpace(sc, "src/main/resources/article/article_9996786.json", 5).collect().foreach(println)

  }
}
