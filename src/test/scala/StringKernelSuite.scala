import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class StringKernelSuite extends FunSuite with BeforeAndAfterAll {

  var spark: SparkContext = _

  override def beforeAll(): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("StringKernelSuite")
    spark = new SparkContext(conf)
    spark.setLogLevel("WARN")
  }

  test("ssk") {
    val lbda = 0.6

    assert(math.abs(
      StringKernel.ssk("cat", "cart", 4, lbda) -
        (3*math.pow(lbda, 2) + math.pow(lbda, 4) + math.pow(lbda, 5) + 2*math.pow(lbda, 7))) < 1e-6)

    assert(StringKernel.ssk("science is organized knowledge", "wisdom is organized life", 4, 1) == 20538.0)

    val test = "This is a very long string, just to test how fast this implementation of ssk is. It should look like the computation tooks no time, unless you're running this in a potato pc"

    print(StringKernel.ssk(test, "This is a very long string", 30, .8) )
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

}
