package com.ebay.hadoop.spark

import org.apache.spark.sql.SparkSession

case class Config(parquetPath: String = null, sql: String = null)

object BenchMark {
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("as") {
      head("spark benchmark", "0.0.1-SNAPSHOT")
      opt[String]('p', "path")
        .required()
        .action((path, config) => config.copy(parquetPath = path))
        .text("Input path")

      opt[String]('s', "sql")
        .required()
        .action((s, config) => config.copy(sql = s))
        .text("SQL to run")
    }

    parser.parse(args, Config()) match {
      case Some(config) => run(config)
      case None => System.exit(1)
    }
  }

  def run(config: Config): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark bench")
      .getOrCreate()

    val df = spark.read.parquet(config.parquetPath)

    df.createOrReplaceTempView("t")

    spark.sql(config.sql)
      .collect()
      .foreach(println)
  }
}

