package com.ebay.hadoop.arrow.executor

import java.io.{FileInputStream, FileOutputStream}

import com.ebay.hadoop.arrow.executor.plan.ArrowPlan.Plan
import com.google.protobuf.util.JsonFormat
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PlanGeneratorSuite extends FunSuite {
  test("Generate plan") {
    val path = "/root/liu/spark/data/10m/part-00006-246ebed7-c6b3-4004-a1a9-7fb6d892cc68-c000.snappy.parquet"

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
         |              "name": "vstr_yn_id"
         |            },
         |            {
         |              "basic_type": "INT32",
         |              "nullable": true,
         |              "name": "flags01"
         |            },
         |            {
         |              "basic_type": "INT64",
         |              "nullable": true,
         |              "name": "session_skey"
         |            },
         |            {
         |              "basic_type": "INT32",
         |              "nullable": true,
         |              "name": "seqnum"
         |            },
         |            {
         |              "basic_type": "INT32",
         |              "nullable": true,
         |              "name": "cobrand"
         |            },
         |            {
         |              "basic_type": "INT64",
         |              "nullable": true,
         |              "name": "source_impr_id"
         |            },
         |            {
         |              "basic_type": "INT64",
         |              "nullable": true,
         |              "name": "agent_id"
         |            },
         |            {
         |              "basic_type": "INT32",
         |              "nullable": true,
         |              "name": "watch_page_id"
         |            },
         |            {
         |              "basic_type": "INT64",
         |              "nullable": true,
         |              "name": "soj_watch_flags64"
         |            },
         |            {
         |              "basic_type": "INT32",
         |              "nullable": true,
         |              "name": "has_dw_rec"
         |            },
         |            {
         |              "basic_type": "INT32",
         |              "nullable": true,
         |              "name": "bot_ind"
         |            },
         |            {
         |              "basic_type": "INT64",
         |              "nullable": true,
         |              "name": "del_session_skey"
         |            },
         |            {
         |              "basic_type": "INT32",
         |              "nullable": true,
         |              "name": "del_seqnum"
         |            },
         |            {
         |              "basic_type": "INT32",
         |              "nullable": true,
         |              "name": "del_cobrand"
         |            },
         |            {
         |              "basic_type": "INT64",
         |              "nullable": true,
         |              "name": "del_agent_id"
         |            },
         |            {
         |              "basic_type": "INT32",
         |              "nullable": true,
         |              "name": "del_page_id"
         |            },
         |            {
         |              "basic_type": "INT64",
         |              "nullable": true,
         |              "name": "del_flags64"
         |            },
         |            {
         |              "basic_type": "INT32",
         |              "nullable": true,
         |              "name": "del_bot_ind"
         |            },
         |            {
         |              "basic_type": "INT64",
         |              "nullable": true,
         |              "name": "format_flags64"
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

    val filename = "/Users/renliu/Downloads/plan.proto"
    planBuilder.build().writeTo(new FileOutputStream(filename))

    println(JsonFormat.printer().print(Plan.parseFrom(new FileInputStream(filename))))
  }
}
