package standardscala

import scalafx.application.JFXApp
import scalafx.collections.ObservableBuffer
import scalafx.scene.Scene
import scalafx.scene.chart.{NumberAxis, ScatterChart, XYChart}

object PlotTemps extends JFXApp {
  private val source = scala.io.Source.fromFile("MN212142_9392.csv.txt")
  private val lines = source.getLines().drop(1)
  private val data = lines.flatMap { line =>
    val p = line.split(",")
    if (p(7) == "." || p(8) == "." || p(9) == ".") Seq.empty else
      Seq(TempData(p(0).toInt, p(1).toInt, p(2).toInt, p(4).toInt,
        TempData.toDoubleOrNeg(p(5)), TempData.toDoubleOrNeg(p(6)), p(7).toDouble, p(8).toDouble,
        p(9).toDouble))
  }.toArray
  source.close()

  stage = new JFXApp.PrimaryStage {
    title = "Temp Plot"
    scene = new Scene(1000, 1000) {
      private val xAxis = NumberAxis()
      private val yAxis = NumberAxis()
      private val pData = XYChart.Series[Number, Number]("Temps",
        ObservableBuffer(data.map(td => XYChart.Data[Number, Number](td.doy, td.tmin)): _*))
      val plot = new ScatterChart(xAxis, yAxis, ObservableBuffer(pData))
      root = plot
    }
  }
}
