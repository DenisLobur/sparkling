package standardscala

import scala.io.Source

case class TempData(day: Int, doy: Int, month: Int, year: Int,
                    precip: Double, snow: Double, tav: Double, tmax: Double, tmin: Double)

object TempData {

  def toDoubleOrNeg(s: String): Double = {
    try {
      s.toDouble
    } catch {
      case _: NumberFormatException => -1
    }
  }

  def main(args: Array[String]): Unit = {
    val source = Source.fromFile("MN212142_9392.csv.txt")
    val lines = source.getLines().drop(1)
    val data = lines.flatMap { line =>
      val p = line.split(",")
      if (p(7).equals(".") || p(8).equals(".") || p(9).equals(".")) Seq.empty else
        Seq(TempData(p(0).toInt, p(1).toInt, p(2).toInt, p(4).toInt,
          toDoubleOrNeg(p(5)), toDoubleOrNeg(p(6)), p(7).toDouble, p(8).toDouble, p(9).toDouble))
    }.toArray
    source.close()

    calcMaxTemp0(data)
    calcMaxTemp1(data)
    calcMaxTemp2(data)
    calcPrecipitationDaysFraction(data)
    calcAverageTempOnRainyDays0(data)
    calcAverageTempOnRainyDays1(data)
    calcAverageTempOnRainyDaysPar(data)
    calcAverageTempByMonths(data)
  }

  /**
    * Calculate max temperature through traversing all dataset several times.
    * Returns an array of hot days
    *
    * @param data
    */
  def calcMaxTemp0(data: Array[TempData]): Unit = {
    val maxTemp = data.map(_.tmax).max
    val hotDays = data.filter(_.tmax == maxTemp)
    println(s"Hot days: ${hotDays.mkString(", ")}")
  }

  /**
    * Calculate max temperature through one traverse using 'maxBy'.
    * Returns one (first) TempData
    *
    * @param data
    */
  def calcMaxTemp1(data: Array[TempData]): Unit = {
    val hotDay = data.maxBy(_.tmax)
    println(s"Hot day 1 is $hotDay")
  }

  /**
    * Calculate max temperature through one traverse using 'reduceLeft'.
    * Returns one (first) TempData
    *
    * @param data
    */
  def calcMaxTemp2(data: Array[TempData]): Unit = {
    val hotDay2 = data.reduceLeft((d1, d2) => if (d1.tmax >= d2.tmax) d1 else d2)
    println(s"Hot day 2 is $hotDay2")
  }

  /**
    * Calculate number of days when precipitation was more than 1 inch. And fraction of those days to all dataset
    *
    * @param data
    */
  def calcPrecipitationDaysFraction(data: Array[TempData]): Unit = {
    val rainyCount = data.count(_.precip >= 1)
    println(s"There are $rainyCount rainy days. That is ${rainyCount * 100.0 / data.length} percent")
  }

  /**
    * Calculate average temperature on all rainy days using 'foldLeft' with accumulator as a tuple
    *
    * @param data
    */
  def calcAverageTempOnRainyDays0(data: Array[TempData]): Unit = {
    val start = System.nanoTime()
    val (rainySum, rainyCount2) = data.foldLeft(0.0 -> 0) { case ((sum, cnt), td) =>
      if (td.precip < 1.0) (sum, cnt) else (sum + td.tmax, cnt + 1)
    }
    val end = System.nanoTime()
    println(s"Average temp on rainy day is ${rainySum / rainyCount2} calculated for ${end - start} milliseconds")
  }

  /**
    * Calculate average temperature on all rainy days using 'flatMap'
    *
    * @param data
    */
  def calcAverageTempOnRainyDays1(data: Array[TempData]): Unit = {
    val start = System.nanoTime()
    val rainyTemps = data.flatMap(td => if (td.precip < 1.0) Seq.empty else Seq(td.tmax))
    val end = System.nanoTime()
    println(s"Average temp on rainy day is ${rainyTemps.sum / rainyTemps.length} calculated for ${end - start} milliseconds")
  }

  /**
    * Calculate average temperature on all rainy days using 'aggregate' in parallel
    *
    * @param data
    */
  def calcAverageTempOnRainyDaysPar(data: Array[TempData]): Unit = {
    val start = System.nanoTime()
    val (rainySum, rainyCount2) = data.par.aggregate(0.0 -> 0)(
      { case ((sum, cnt), td) =>
        if (td.precip < 1.0) (sum, cnt) else (sum + td.tmax, cnt + 1)
      },
      {
        case ((sum1, cnt1), (sum2, cnt2)) =>
          (sum1 + sum2, cnt1 + cnt2)
      })
    val end = System.nanoTime()
    println(s"Average temp on rainy day is ${rainySum / rainyCount2} calculated for ${end - start} milliseconds [Parallel calculation]")
  }

  /**
    * Calculate average temperature by months using 'groupBy', 'map', 'foldLeft' and 'sortBy'
    *
    * @param data
    */
  def calcAverageTempByMonths(data: Array[TempData]): Unit = {
    val monthGroups = data.groupBy(_.month)
    val monthlyTemp = monthGroups.map { case (m, days) =>
      m -> days.foldLeft(0.0)((sum, td) => sum + td.tmax / days.length)
    }
    monthlyTemp.toSeq.sortBy(_._1) foreach println
  }
}
