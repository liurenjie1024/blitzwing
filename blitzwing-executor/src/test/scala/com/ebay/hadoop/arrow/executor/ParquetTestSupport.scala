package com.ebay.hadoop.arrow.executor

import java.io.File

import org.apache.commons.io.FileUtils

trait ParquetTestSupport {
  def withTmpDir(f: File => Unit): Unit = {
    val dir = TestUtils.createTempDir()
    try {
      f(dir)
    } finally {
      FileUtils.deleteDirectory(dir)
    }
  }
}
