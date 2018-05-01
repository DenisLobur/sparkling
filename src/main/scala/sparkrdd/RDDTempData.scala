package sparkrdd

import org.apache.spark.{SparkConf, SparkContext}
import standardscala.PlotTemps.lines
import standardscala.TempData

object RDDTempData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Temp Data").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("MN212142_9392.csv.txt").filter(!_.contains("Day"))

    val data = lines.flatMap { line =>
      val p = line.split(",")
      if (p(7) == "." || p(8) == "." || p(9) == ".") Seq.empty else
        Seq(TempData(p(0).toInt, p(1).toInt, p(2).toInt, p(4).toInt,
          TempData.toDoubleOrNeg(p(5)), TempData.toDoubleOrNeg(p(6)), p(7).toDouble, p(8).toDouble,
          p(9).toDouble))
    }.cache() // use cache() so spark does not recompute 'data' on every use

    //data.take(5) foreach println
    println(data.max()(Ordering.by(_.tmax)))
    println(data.reduce((td1, td2) => if (td1.tmax >= td2.tmax) td1 else td2))

    val rainyCount = data.filter(_.precip >= 1).count()
    println(s"There are $rainyCount rainy days. That is ${rainyCount * 100.0 / data.count()} percent")

    val (rainySum, rainyCount2) = data.aggregate(0.0 -> 0)(
      { case ((sum, cnt), td) =>
        if (td.precip < 1.0) (sum, cnt) else (sum + td.tmax, cnt + 1)
      },
      {
        case ((sum1, cnt1), (sum2, cnt2)) =>
          (sum1 + sum2, cnt1 + cnt2)
      })
    println(s"Average temp on rainy day is ${rainySum / rainyCount2}")

    val rainyTemps = data.flatMap(td => if (td.precip < 1.0) Seq.empty else Seq(td.tmax))
    val end = System.nanoTime()
    println(s"Average temp on rainy day is ${rainyTemps.sum / rainyTemps.count()}")


    val monthGroups = data.groupBy(_.month)
    val monthlyTemp = monthGroups.map { case (m, days) =>
      m -> days.foldLeft(0.0)((sum, td) => sum + td.tmax) / days.size
    }
    monthlyTemp.collect().sortBy(_._1) foreach println
  }
}
