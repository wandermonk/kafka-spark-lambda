package clickstream

import java.io.FileWriter

import config.Settings
import org.apache.commons.io.FileUtils

import scala.util.Random

object LogProducer extends App {

  //weblog gen config reference
  val wlc = Settings.WebLogGen

  val Products = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/products.csv")).getLines().toArray
  val Referrers = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/referrers.csv")).getLines().toArray

  val Visitors = (0 to wlc.visitors).map("Visitor-"+_)
  var Pages = (0 to wlc.pages).map("Page-"+_)

  val rnd = new Random()
  val file = wlc.filePath
  val destPath = wlc.destPath

  for(fileCount <- 0 to wlc.numFiles) {

    val fileWriter = new FileWriter(file, true)
    val incrementTimeFrequency = rnd.nextInt(wlc.records - 1) + 1

    var timeStamp = System.currentTimeMillis()
    var adjustedTimeStamp = timeStamp

    for (iteration <- 1 to wlc.records) {
      adjustedTimeStamp = adjustedTimeStamp + ((System.currentTimeMillis() - timeStamp) * wlc.timeMultiplier)
      timeStamp = System.currentTimeMillis()
      val action = iteration % (rnd.nextInt(200) + 1) match {
        case 0 => "purchase"
        case 1 => "add_to_cart"
        case _ => "pageview"
      }

      val referrer = Referrers(rnd.nextInt(Referrers.length - 1))
      val prevPage = referrer match {
        case "internal" => Pages(rnd.nextInt(Pages.length - 1))
        case _ => " "
      }

      val visitor = Visitors(rnd.nextInt(Visitors.length - 1))
      val page = Pages(rnd.nextInt(Pages.length - 1))
      val product = Products(rnd.nextInt(Products.length - 1))

      val line = s"$adjustedTimeStamp\t$referrer\t$action\t$prevPage\t$visitor\t$page\t$product\n"
      fileWriter.write(line)

      if (iteration % incrementTimeFrequency == 0) {
        println(s"Sent $iteration messages!")
        val sleeping = rnd.nextInt(incrementTimeFrequency * 60)
        println(s"sleeping for $sleeping ms")
        Thread.sleep(sleeping)
      }
    }
    fileWriter.close()
    val outputFile = FileUtils.getFile(s"${destPath}data_$timeStamp")
    println(s"Moving produced data to $outputFile")
    FileUtils.moveFile(FileUtils.getFile(file),outputFile)
    val sleeping = 5000
    println(s"sleeping for $sleeping")
  }
}
