import org.apache.spark.{SparkConf, SparkContext}

object Main {

  private var input: String = _
  private var output: String = _
  private var n: Int = _
  private var lbda: Double = _
  private var numPartitions: Int = 30

  def main(args: Array[String]): Unit = {

    require(args.length >= 4)

    input = args(0)
    output = args(1)
    n = args(2).toInt
    lbda = args(3).toDouble
    if (args.length >= 5) {
      numPartitions = args(4).toInt
    }

    val conf = new SparkConf()
//      .setMaster("local[*]")
      .setAppName("StringKernel")
    val spark = new SparkContext(conf)
    spark.setLogLevel("WARN")

    val data = spark.textFile(input, numPartitions).map {line =>
      val tokens = line.split("\t")
      (tokens(0).toInt, tokens(1))
    }

    val ssk = new StringKernel()
      .setN(n)
      .setLbda(lbda)

    val sims = ssk.run(data)

    sims.map{ case (essayId, sim) =>
      essayId + "\t" + sim.toArray.mkString(",")
    }.saveAsTextFile(output)
  }

}
