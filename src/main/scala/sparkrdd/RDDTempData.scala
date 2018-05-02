package sparkrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import standardscala.TempData
import swiftvis2.plotting.Plot
import scalafx.application.JFXApp
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer

object RDDTempData extends JFXApp {
  val conf = new SparkConf().setAppName("Temp Data").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

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
  val monthlyHighTemp = monthGroups.map { case (m, days) =>
    m -> days.foldLeft(0.0)((sum, td) => sum + td.tmax) / days.size
  }
  val monthlyLowTemp = monthGroups.map { case (m, days) =>
    m -> days.foldLeft(0.0)((sum, td) => sum + td.tmin) / days.size
  }
  monthlyHighTemp.collect().sortBy(_._1) foreach println

  calculateStDev(data)
  //plotHightTempHistogram(data)
  //scateredPlot(data, monthlyHighTemp, monthlyLowTemp)
  averageTempByYearPlot()


  private def calculateStDev(data: RDD[TempData]): Unit = {
    println("Standard deviation of tmax: " + data.map(_.tmax).stdev())
    println("Standard deviation of tmin: " + data.map(_.tmin).stdev())
    println("Standard deviation of taverage: " + data.map(_.tav).stdev())
  }

  private def plotHightTempHistogram(data: RDD[TempData]): Unit = {
    val bins = (-20.0 to 107.0 by 1.0).toArray
    val counts = data.map(_.tmax).histogram(bins, true)
    val hist = Plot.histogramPlot(bins, counts, RedARGB, false)
    FXRenderer(hist, 800, 600)
  }

  private def scateredPlot(data: RDD[TempData],
                           monthlyHighTemp: RDD[(Int, Double)],
                           monthlyLowTemp: RDD[(Int, Double)]): Unit = {
    val plot = Plot.scatterPlots(Seq(
      (monthlyHighTemp.map(_._1).collect(), monthlyHighTemp.map(_._2).collect(), RedARGB, 5),
      (monthlyLowTemp.map(_._1).collect(), monthlyLowTemp.map(_._2).collect(), BlueARGB, 5)
    ), "Temps", "Month", "Temperature")
    FXRenderer(plot, 800, 600)
  }

  private def averageTempByYearPlot(): Unit ={
    val keyedByYear = data.map(td => td.year -> td)
    val averageTempsByYear = keyedByYear.aggregateByKey(0.0 -> 0)({ case ((sum, cnt), td) =>
      (sum+td.tmax, cnt+1)
    }, { case ((s1, c1), (s2, c2)) => (s1+s2, c1+c2) })

    val averageByYearData = averageTempsByYear.collect().sortBy(_._1)
    val longTermPlot = Plot.scatterPlotWithLines(averageByYearData.map(_._1),
      averageByYearData.map { case (_, (s, c)) => s/c }, "Average temperature", symbolSize = 0, lineGrouping = 1)
    FXRenderer(longTermPlot, 800, 600)
  }
}
