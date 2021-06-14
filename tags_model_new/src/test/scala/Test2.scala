import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

object Test2 {
  def main(args: Array[String]): Unit = {
    def catalogRead =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_users"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"username":{"cf":"default", "col":"username", "type":"string"}
         |}
         |}""".stripMargin

    val spark = SparkSession.builder()
      .appName("shc test")
      .master("local[10]")
      .getOrCreate()

    val readDF = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalogRead)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    def catalogWrite =
      s"""{
         |"table":{"namespace":"default", "name":"tbl_users_test"},
         |"rowkey":"id",
         |"columns":{
         |"id":{"cf":"rowkey", "col":"id", "type":"string"},
         |"username":{"cf":"default", "col":"username", "type":"string"}
         |}
         |}""".stripMargin

    readDF.write
      .option(HBaseTableCatalog.tableCatalog, catalogWrite)
      .option(HBaseTableCatalog.newTable, "5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }
}
