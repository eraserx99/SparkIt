import org.scalatest.fixture.FunSuite
import org.apache.spark.{SparkConf, SparkContext}

class HaveMoreFun extends FunSuite {

  case class S(conf: SparkConf, sc: SparkContext)

  type FixtureParam = S

  def withFixture(test: OneArgTest) = {
    val conf = new SparkConf().setMaster("local").setAppName("SparkIt")
    val sc = new SparkContext(conf)

    try {
      test(S(conf, sc))
    } finally {
      sc.stop();
    }
  }
}
