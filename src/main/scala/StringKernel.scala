import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

class StringKernel(
                    private var n: Int,
                    private var lbda: Double,
                    private var accum: Boolean
                  ) extends Serializable {

  def this() = this(5, 0.8, true)

  def getN: Int = n

  def setN(value: Int): this.type = {
    n = value
    this
  }

  def setLbda(value: Double): this.type = {
    lbda = value
    this
  }

  def setAccum(value: Boolean): this.type = {
    accum = value
    this
  }

  def run(data: RDD[(Int, String)]): RDD[(Int, Vector)] = {

    val numPartitions = data.getNumPartitions

    val cnt = data.count()

    data.zipWithIndex().flatMap { case ((essayId, essay), id) => {
      0L until cnt
    }.map { otherId =>
      if (id <= otherId) {
        ((id, otherId), (essayId, essay))
      } else {
        ((otherId, id), (essayId, essay))
      }
    }
    }.groupByKey(numPartitions)
      .flatMap { case (_, iter) =>
        val essays = iter.toArray
        val sim = StringKernel.ssk2(essays.head._2, essays.last._2, n, lbda)
        val ret = ArrayBuffer(Tuple2(essays.head._1, (essays.last._1, sim)))
        if (essays.head._1 != essays.last._1) {
          ret += Tuple2(essays.last._1, (essays.head._1, sim))
        }
        ret
      }.groupByKey(numPartitions).map { case (essayId, iter) =>
      val sims = iter.toArray.sorted.map(_._2)
      (essayId, Vectors.dense(sims))
    }.sortByKey()
  }

  def run2(data: RDD[(Int, String)], reference: Array[(Int, String)]): RDD[(Int, Vector)] = {
    val spark = data.sparkContext

    val referenceBC = spark.broadcast(reference)

    data.mapPartitions { iter =>
      val reference = referenceBC.value
      iter.map { case (essayId, essay) =>
        println(s"Computing $essayId")
        val similarities = new Array[Double](reference.length)
        for (i <- reference.indices) {
          similarities(i) = StringKernel.ssk2(essay, reference(i)._2, n, lbda)
        }
        (essayId, Vectors.dense(similarities))
      }
    }.sortByKey()
  }
}

object StringKernel extends Serializable {

  def ssk(s: String, t: String, n: Int, lbda: Double): Double = {
    val lens = s.length
    val lent = t.length

    val kprim = Array.ofDim[Double](n, lens, lent)

    // Init
    for (i <- 0 until n) {
      for (j <- 0 until lens) {
        for (k <- 0 until lent) {
          if (i == 0) {
            kprim(i)(j)(k) = 1.0
          } else {
            kprim(i)(j)(k) = 0.0
          }
        }
      }
    }

    for (i <- 1 until n) {
      for (sj <- i until lens) {
        var toret = 0.0
        for (tk <- i until lent) {
          if (s(sj - 1) == t(tk - 1)) {
            toret = lbda * (toret + lbda * kprim(i - 1)(sj - 1)(tk - 1))
          } else {
            toret *= lbda
          }
          kprim(i)(sj)(tk) = toret + lbda * kprim(i)(sj - 1)(tk)
        }
      }
    }

    var k = 0.0

    for (i <- 0 until n) {
      for (sj <- i until lens) {
        for (tk <- i until lent) {
          if (s(sj) == t(tk)) {
            k += lbda * lbda * kprim(i)(sj)(tk)
          }
        }
      }
    }

    k
  }

  def ssk2(s: String, t: String, n: Int, lbda: Double): Double = {
    val lens = s.length
    val lent = t.length

    var prevkprim = Array.ofDim[Double](lens, lent)
    var kprim = Array.ofDim[Double](lens, lent)
    var sim = 0.0

    // Init
    for (j <- 0 until lens) {
      for (k <- 0 until lent) {
        prevkprim(j)(k) = 1.0
        kprim(j)(k) = 0.0
        if (s(j) == t(k)) {
          sim += lbda * lbda * prevkprim(j)(k)
        }
      }
    }

    for (i <- 1 until n) {
      for (sj <- i until lens) {
        var toret = 0.0
        for (tk <- i until lent) {
          if (s(sj - 1) == t(tk - 1)) {
            toret = lbda * (toret + lbda * prevkprim(sj - 1)(tk - 1))
          } else {
            toret *= lbda
          }
          kprim(sj)(tk) = toret + lbda * kprim(sj - 1)(tk)
          if (s(sj) == t(tk)) {
            sim += lbda * lbda * kprim(sj)(tk)
          }
        }
      }
      val tmp = prevkprim
      prevkprim = kprim
      kprim = tmp
    }

    sim
  }

}