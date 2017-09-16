package config

import com.typesafe.config.ConfigFactory

object Settings {
  private val config = ConfigFactory.load()

  object WebLogGen {
    private val webloggen = config.getConfig("clickstream")

    lazy val records = webloggen.getInt("records")
    lazy val timeMultiplier = webloggen.getInt("time_multiplier")
    lazy val pages = webloggen.getInt("pages")
    lazy val visitors = webloggen.getInt("visitors")
    lazy val filePath = webloggen.getString("file_path")
  }
}
