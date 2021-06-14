package edu.hut.models.matchModels

import com.typesafe.config.ConfigFactory
import edu.hut.utils.SHCUtils
import edu.hut.{CommonMeta, HBaseMeta, MetaData, Tag}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.util.Properties



object GenderModel {
  val TAG_NAME = "性别"
  val HBASE_NAMESPACE = "default"
  val HBASE_ROWKEY_FIELD = "id"
  val HBASE_COLUMN_DEFAULT_TYPE = "string"
  val HBASE_USER_PROFILE = "user_profile_new"

  val spark = SparkSession.builder()
    .appName("gender model")
    .master("local[5]")
    .getOrCreate()

  val config = ConfigFactory.load()

  def main(args: Array[String]): Unit = {
    // 1. 读取 MySQL 的四级标签和五级标签数据
    val (fourTag, fiveTags) = readBasicTag()

    // 2. 通过四级标签, 读取元数据
    // 3. 处理元数据, 处理成结构化的方式
    val metaData = readMetaData(fourTag.id)

    // 4. 使用元数据, 连接源表, 拿到源表数据
    val (source, commonMeta) = createSource(metaData)

    // 5. 匹配计算标签数据
    // 是将五级标签和数据进行匹配

    // 男,50,1   ->     id,gender
    val result = process(source, fiveTags, commonMeta.outFields)

    // 6. 把标签信息, 放入用户画像表
    saveUserProfile(result, commonMeta.outFields)
  }

  /**
   * 读取 MySQL 中的四级标签和五级标签信息
   */
  def readBasicTag(): (Tag, Array[Tag]) = {
    // 1.1. 创建配置文件, 默认 load 方法不指定配置文件读取的就是 application.conf
    val url = config.getString("jdbc.basic_tag.url")
    val table = config.getString("jdbc.basic_tag.table")

    // 1.2. 创建 SparkSession, 读取四级标签
    val source: DataFrame = spark.read.jdbc(url, table, new Properties())

    // 可以通过name筛选对应的四级标签
    import spark.implicits._

    val fourTag = source.where('name === TAG_NAME) // where name = "性别"
      .as[Tag]
      .collect()
      .head

    // 1.3. 读取五级标签, 使用四级标签的id, 作为五级标签的pid, 去查询五级标签
    val fiveTag = source.where('pid === fourTag.id)
      .as[Tag]
      .collect()

    (fourTag, fiveTag)
  }

  /**
   * 读取并处理元数据
   * 系统中支持三种类型的元数据: RDBMS, HDFS, HBase
   * 所以要针对这些类型进行单独的处理
   * 所以要区分开, 不同的元数据类型的数据
   */
  def readMetaData(fourTagID: String): MetaData = {
    import org.apache.spark.sql.functions._
    import spark.implicits._

    // 1. 元数据表的配置
    val url = config.getString("jdbc.basic_tag.url")
    val table = config.getString("jdbc.meta_data.table")
    val matchColumn = config.getString("jdbc.meta_data.match_column")

    // 2. 读取元数据
    val metaData = spark.read
      .jdbc(url, table, new Properties())
      .where(col(matchColumn) === fourTagID)
      .as[MetaData]
      .collect()
      .head

    // 3. 解析元数据
    metaData
  }

  /**
   * 根据元数据的类型, 读取不同的数据库中的表
   *
   * @param metaData 元数据
   */
  def createSource(metaData: MetaData): (DataFrame, CommonMeta) = {
    // 判断是否是 HBase
    if (metaData.isHBase) {
      val hbaseMeta: HBaseMeta = metaData.toHBaseMeta
      val source: DataFrame = SHCUtils.read(hbaseMeta.tableName, hbaseMeta.commonMeta.inFields, hbaseMeta.columnFamily, spark)

      return (source, hbaseMeta.commonMeta)
    }

    // 判断是否是 MySQL
    if (metaData.isRDBMS) {

    }

    // 判断是否是 HDFS
    if (metaData.isHDFS) {

    }

    (null, null)
  }

  /**
   * 匹配计算
   */
  def process(source: DataFrame, fiveTags: Array[Tag], outFields: Array[String]): DataFrame = {
    import org.apache.spark.sql.functions._
    import spark.implicits._

    // 1. 拼接匹配规则, 五级标签的 rule 就是匹配用的值
    var conditions: Column = null

    for (tag <- fiveTags) {
      conditions = if (conditions == null)
        when('gender === tag.rule, tag.id)
      else
        conditions.when('gender === tag.rule, tag.id)
    }

    conditions = conditions.as(outFields.head)

    // 2. 执行规则, 过滤数据     select id, case when gender == rule then tag_id as tag_gender from ...
    source.select('id, conditions)
  }

  /**
   * 存储画像数据
   */
  def saveUserProfile(result: DataFrame, outFields: Array[String]): Unit = {
    SHCUtils.writeToHBase(outFields, result, "5")
  }
}
