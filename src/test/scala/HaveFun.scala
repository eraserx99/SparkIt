import org.scalatest.fixture.FunSuite
import org.apache.spark.{SparkConf, SparkContext}

class HaveFun extends FunSuite {

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

  /**
    * Transformations
    */

  test("groupByKey") { s =>
    val sc = s.sc
    val data = sc.parallelize(Array(('k', 5), ('s', 3), ('s', 4), ('p', 7), ('p', 5), ('t', 8), ('k', 6)), 3)
    val group = data.groupByKey()

    /**
      * group => Array((s,CompactBuffer(3, 4)), (p,CompactBuffer(7, 5)), (t,CompactBuffer(8)), (k,CompactBuffer(5, 6)))
      */
    assert(group.collect().length == 4)
  }

  test("reduceByKey") { s =>
    val sc = s.sc
    val words = Array("one", "two", "two", "four", "five", "six", "six", "eight", "nine", "ten")
    val data = sc.parallelize(words).map(w => (w, 1)).reduceByKey(_ + _)

    /**
      * data => Array((two,2), (eight,1), (one,1), (nine,1), (six,2), (five,1), (ten,1), (four,1))
      */
    assert(data.collect().length == 8)
  }

  test("sortByKey") { s =>
    val sc = s.sc
    val data = sc.parallelize(Seq(("maths", 52), ("english", 75), ("science", 82), ("computer", 65), ("maths", 85)))
    val sorted = data.sortByKey()
    val (course, score) = sorted.take(1)(0)

    assert(course == "computer" && score == 65)
  }

  test("join") { s =>
    val sc = s.sc
    val data = sc.parallelize(Array(('A', 1), ('b', 2), ('c', 3)))
    val data2 = sc.parallelize(Array(('A', 4), ('A', 6), ('b', 7), ('c', 3), ('c', 8)))
    val result = data.join(data2)

    /**
      * result => Array((b,(2,7)), (A,(1,4)), (A,(1,6)), (c,(3,3)), (c,(3,8)))
      */
    assert(result.collect().length == 5)
  }

  /**
    * Actions
    */

  test("countByValue") { s =>
    val sc = s.sc
    val data = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 6, 7))
    val result = data.countByValue()

    /**
      * result => Map(5 -> 1, 1 -> 1, 6 -> 2, 2 -> 1, 7 -> 1, 3 -> 1, 4 -> 1)
      */
    assert(result(6) == 2)
  }

  test("reduce") { s=>
    val sc = s.sc
    val rdd1 = sc.parallelize(List(20, 32, 4))

    /**
      * commutative and associative operations
      */
    val sum = rdd1.reduce(_+_)

    assert(sum == 56)
  }

  test("top") { s=>
    val sc = s.sc
    val rdd1 = sc.parallelize(List(6, 27, 13, 19))
    val leaders = rdd1.top(2)

    assert(leaders(1) == 19)
  }
}
