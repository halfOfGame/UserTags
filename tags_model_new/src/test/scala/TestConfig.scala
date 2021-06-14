import com.typesafe.config.ConfigFactory

object TestConfig {
  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.load()
    val url = config.getString("jdbc.basic_tag.url")
    println(url)
  }
}
