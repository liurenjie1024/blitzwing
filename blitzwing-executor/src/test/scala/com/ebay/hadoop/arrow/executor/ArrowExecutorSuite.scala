package com.ebay.hadoop.arrow.executor

import java.nio.file.Paths

import com.ebay.hadoop.arrow.executor.plan.ArrowPlan.Plan
import com.github.mjakubowski84.parquet4s.ParquetWriter
import com.google.protobuf.util.JsonFormat
import org.apache.arrow.vector.{BigIntVector, IntVector}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class ArrowExecutorSuite extends FunSuite with ParquetTestSupport {
  case class User(age: Int, childrenNum: Int, salary: Long)
  test("Read parquet file") {
    val users = 0 until 20000 map (_ => User(Random.nextInt(100), Random.nextInt(10), Random.nextInt(100000)))
//    val users = List(
//      User(10, 0, 100),
//      User(20, 1, 1000),
//      User(30, 2, 10000)
//    )

    withTmpDir { dir =>
      val path = Paths.get(dir.getAbsolutePath, "test.parquet")
        .toAbsolutePath
        .toString

      ParquetWriter.write(path, users)

      val jsonPlan =
        s"""
          |{
          |  "plan_nodes": [
          |    {
          |      "node_type": "PARQUET_FILE_SCAN_NODE",
          |      "parquet_file_scan_node": {
          |        "path": "$path",
          |        "schema": {
          |          "fields": [
          |            {
          |              "basic_type": "INT32",
          |              "nullable": true,
          |              "name": "age"
          |            },
          |            {
          |              "basic_type": "INT32",
          |              "nullable": true,
          |              "name": "childrenNum"
          |            },
          |            {
          |              "basic_type": "INT64",
          |              "nullable": true,
          |              "name": "salary"
          |            }
          |          ]
          |        }
          |      }
          |    }
          |  ]
          |}
          |""".stripMargin

      val planBuilder = Plan.newBuilder()
      JsonFormat.parser().merge(jsonPlan, planBuilder)

      val executor = ArrowExecutor(0, planBuilder.build())

      var start = 0
      while (executor.hasNext) {
        val recordBatch = executor.next()

        val ageVector = recordBatch.vectors(0).asInstanceOf[IntVector]
        val childrenNumVector = recordBatch.vectors(1).asInstanceOf[IntVector]
        val salaryVector = recordBatch.vectors(2).asInstanceOf[BigIntVector]

        for (i <- 0 until recordBatch.numRows) {
          assert(users(start+i).age == ageVector.get(i))
          assert(users(start+i).childrenNum == childrenNumVector.get(i))
          assert(users(start+i).salary == salaryVector.get(i))
        }

        start += recordBatch.numRows
      }
    }
  }
}
