import edu.hut.{HBaseCatalog, HBaseTable}

import scala.collection.mutable
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

object Test3 {
  def main(args: Array[String]): Unit = {

  }

  def generateCatalog(rowkeyField: String, columnFamily: String, tableName: String): String = {
//    val columns: mutable.HashMap[String, HBaseField] = mutable.HashMap.empty[String, HBaseField]
//    columns += rowkeyField -> HBaseField("rowkey", rowkeyField, "string")
//    columns += "username" -> HBaseField(columnFamily, "username", "string")
//
//    val hbaseCatalog = HBaseCatalog(HBaseTable("default", tableName), rowkeyField, columns.toMap)
//
//    import org.json4s._
//    import org.json4s.jackson.Serialization
//    import org.json4s.jackson.Serialization.write
//    implicit  val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
//
//    write(hbaseCatalog)
    null
  }
}
