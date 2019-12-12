import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Main2 {

  private var input: String = _
  private var referenceInput: String = _
  private var output: String = _
  private var n: Int = _
  private var lbda: Double = _
  private var numPartitions: Int = -1

  def main(args: Array[String]): Unit = {

    require(args.length >= 5)

    input = args(0)
    referenceInput = args(1)
    output = args(2)
    n = args(3).toInt
    lbda = args(4).toDouble
    if (args.length >= 6) {
      numPartitions = args(5).toInt
    }

    val conf = new SparkConf()
//      .setMaster("local[*]")
      .setAppName("StringKernel")
    val spark = new SparkContext(conf)
    spark.setLogLevel("WARN")

    if (numPartitions <= 0) {
      numPartitions = spark.defaultParallelism
    }

    val data = spark.textFile(input, numPartitions).map {line =>
      val tokens = line.split("\t")
      (tokens(0).toInt, tokens(1))
    }

    val reference = spark.textFile(referenceInput).map {line =>
      val tokens = line.split("\t")
      (tokens(0).toInt, tokens(1))
    }.sortByKey().collect()

    val ssk = new StringKernel()
      .setN(n)
      .setLbda(lbda)

    val sims = ssk.run2(data, reference).persist(StorageLevel.MEMORY_AND_DISK)

    sims.map{ case (essayId, sim) =>
      essayId + "\t" + sim.toArray.mkString(",")
    }.saveAsTextFile(output)

    sims.unpersist()

    spark.stop()
  }

}
