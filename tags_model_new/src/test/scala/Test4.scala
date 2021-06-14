import edu.hut.utils.SHCUtils
import org.apache.spark.sql.SparkSession


/**
 * 使用工具类从HBase中读写数据
 */
object Test4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    val df = SHCUtils.read("tbl_users", Array("username"),"default", spark)

    SHCUtils.writeToHBase(Array("username"), df, "5")
  }
}
