package edu.hut.models.matchModels

import edu.hut.Tag
import edu.hut.models.BasicModel
import org.apache.spark.sql.{Column, DataFrame}

object CitizenModel extends BasicModel{

  def main(args: Array[String]): Unit = {
    startFlow()
  }

  override def tagName(): String = "国籍"

  override def process(source: DataFrame, fiveTags: Array[Tag], outFields: Array[String]): DataFrame = {
    // 1. 导入 Spark 的隐式转换
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 2. 执行匹配计算, 生成条件列
    var conditions: Column = null
    for (tag <- fiveTags) {
      if (conditions == null) {
        conditions = when('nationality === tag.rule, tag.id)
      } else {
        conditions = conditions.when('nationality === tag.rule, tag.id)
      }
    }
    conditions = conditions.as(outFields.head)

    // 3. 在source上筛选, 执行条件
    source.select('id, conditions)
  }
}
