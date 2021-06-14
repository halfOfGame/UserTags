import edu.hut.{HBaseCatalog, HBaseColumn, HBaseTable}

import scala.collection.mutable

object Test5 {
  def main(args: Array[String]): Unit = {
    val table = HBaseTable("default", "tbl_test1")

    val columns = mutable.HashMap.empty[String, HBaseColumn]
    // rowkey 的 column 不能是普通的 CF, 必须得是 rowkey
    columns += "rowkey" -> HBaseColumn("rowkey", "id", "string")
    columns += "userName" -> HBaseColumn("default", "username", "string")

    val catalog = HBaseCatalog(table, "id", columns.toMap)

    // 3. 把 Catalog 转成 JSON 形式
    import org.json4s._
    import org.json4s.jackson.Serialization
    import org.json4s.jackson.Serialization.write

    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

    val catalogJSON = write(catalog)

    println(catalogJSON)
  }
}
