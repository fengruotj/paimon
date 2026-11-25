/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark

import org.apache.paimon.CoreOptions
import org.apache.paimon.data.BinaryRow
import org.apache.paimon.io.DataFileMeta
import org.apache.paimon.manifest.FileSource
import org.apache.paimon.spark.read.BinPackingSplits
import org.apache.paimon.spark.sources.PaimonMicroBatchStream
import org.apache.paimon.table.source.{DataSplit, DeletionFile, QueryAuthSplit, Split}

import org.apache.spark.sql.connector.read.streaming.ReadLimit
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.junit.jupiter.api.Assertions

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConverters._
import scala.collection.mutable

class BinPackingSplitsTest extends PaimonSparkTestBase {

  test("Paimon: reshuffle splits") {
    withSparkSQLConf("spark.sql.files.minPartitionNum" -> "20") {
      val splitNum = 5
      val fileNum = 100

      val files = scala.collection.mutable.ListBuffer.empty[DataFileMeta]
      0.until(fileNum).foreach(i => files += newDataFile(s"f$i.parquet", 750000, 30000, 29999))

      val dataSplits = mutable.ArrayBuffer.empty[Split]
      0.until(splitNum).foreach {
        i =>
          dataSplits += newDataSplitFromFiles(
            files.zipWithIndex.filter(_._2 % splitNum == i).map(_._1).toSeq,
            rawConvertible = true)
      }

      val binPacking = BinPackingSplits(CoreOptions.fromMap(new JHashMap()))
      val reshuffled = binPacking.pack(dataSplits.toArray)
      Assertions.assertTrue(reshuffled.length > 5)
    }
  }

  test("Paimon: reshuffle one split") {
    val split = newDataSplitFromFiles(
      Seq(newDataFile("f1.parquet", 750000, 30000, 29999)),
      rawConvertible = true)

    val dataSplits: Array[Split] = Array(split)

    val binPacking = BinPackingSplits(CoreOptions.fromMap(new JHashMap()))
    val reshuffled = binPacking.pack(dataSplits)
    Assertions.assertEquals(1, reshuffled.length)
  }

  test("Paimon: pack data evolution splits by split granularity") {
    withSparkSQLConf("spark.sql.files.minPartitionNum" -> "1") {
      val split1 = newDataSplit("split1", Seq(40L, 40L), deletionFileLength = Some(5L))
      val split2 = newDataSplit("split2", Seq(40L, 40L), deletionFileLength = Some(5L))
      val split3 = newDataSplit("split3", Seq(40L, 40L), deletionFileLength = Some(5L))

      val binPacking = BinPackingSplits(
        CoreOptions.fromMap(
          Map(
            "data-evolution.enabled" -> "true",
            "deletion-vectors.enabled" -> "true",
            "source.split.open-file-cost" -> "5 B",
            "source.split.target-size" -> "150 B").asJava),
        readRowSizeRatio = 0.5
      )
      val reshuffled = binPacking.pack(Array[Split](split1, split2, split3))

      // Each split size is 2 * (40 * 0.5 + 5 open cost + 5 deletion file length) = 60.
      // Therefore two whole splits fit into the 150 B target while three do not.
      Assertions.assertEquals(2, reshuffled.length)
      Assertions.assertEquals(2, reshuffled.head.splits.length)
      Assertions.assertSame(split1, reshuffled.head.splits.head)
      Assertions.assertSame(split2, reshuffled.head.splits(1))
      Assertions.assertEquals(1, reshuffled(1).splits.length)
      Assertions.assertSame(split3, reshuffled(1).splits.head)
      reshuffled.flatMap(_.splits).foreach {
        split => Assertions.assertEquals(2, split.asInstanceOf[DataSplit].dataFiles().size())
      }
    }
  }

  test("Paimon: data evolution split packing keeps oversized split whole") {
    withSparkSQLConf("spark.sql.files.minPartitionNum" -> "1") {
      val split = newDataSplit("oversized", Seq(40L, 40L))

      val binPacking = BinPackingSplits(
        CoreOptions.fromMap(
          Map(
            "data-evolution.enabled" -> "true",
            "source.split.open-file-cost" -> "0 B",
            "source.split.target-size" -> "50 B").asJava))
      val reshuffled = binPacking.pack(Array[Split](split))

      Assertions.assertEquals(1, reshuffled.length)
      Assertions.assertEquals(1, reshuffled.head.splits.length)
      Assertions.assertSame(split, reshuffled.head.splits.head)
      Assertions.assertEquals(
        2,
        reshuffled.head.splits.head.asInstanceOf[DataSplit].dataFiles().size())
    }
  }

  test("fixed-count packing balances independent files and preserves whole splits") {
    val packing = BinPackingSplits(
      CoreOptions.fromMap(
        Map("source.split.open-file-cost" -> "0 B", "deletion-vectors.enabled" -> "true").asJava))
    val raw = newDataSplit(
      "raw",
      Seq(10, 9, 8, 7, 6, 5),
      rawConvertible = true,
      deletionFileLength = Some(1))
    raw.deletionFiles().get().set(2, null)
    val files = raw.dataFiles().asScala.toList
    val deletions = raw.deletionFiles().get().asScala.toList
    for (n <- Seq(1, 3, 8, Int.MaxValue)) {
      val groups = packing.packForParallelism(Array(raw), n)
      assert(groups.size == math.min(n, files.size))
      val subSplits = groups.flatMap(_.splits).map(_.asInstanceOf[DataSplit])
      val actual =
        subSplits.flatMap(ds => ds.dataFiles().asScala.zip(ds.deletionFiles().get().asScala))
      assert(actual.size == files.size)
      assert(actual.toMap == files.zip(deletions).toMap)
      if (n == 3) {
        assert(
          groups
            .map(
              _.splits.flatMap(_.asInstanceOf[DataSplit].dataFiles().asScala).map(_.fileSize()).sum)
            .sorted == Seq(15L, 15L, 15L))
      }
    }
    assert(packing.packForParallelism(Array.empty[Split], 8).isEmpty)
    val noDeletes = newDataSplit("no-deletes", Seq(1, 2, 3), rawConvertible = true)
    packing.packForParallelism(Array(noDeletes), 2).flatMap(_.splits).foreach {
      split =>
        val ds = split.asInstanceOf[DataSplit]
        assert(ds.deletionFiles().get().asScala.toList == List.fill(ds.dataFiles().size())(null))
    }
    val disabled =
      BinPackingSplits(CoreOptions.fromMap(Map("deletion-vectors.enabled" -> "false").asJava))
    assert(disabled.packForParallelism(Array(raw), 2).flatMap(_.splits).forall {
      split => !split.asInstanceOf[DataSplit].deletionFiles().isPresent
    })

    val merge = newDataSplit("merge", Seq(0, 0))
    val authorized = new QueryAuthSplit(raw, null)
    val mixed = packing.packForParallelism(Array(merge, authorized, raw), 2)
    assert(mixed.size == 2)
    assert(mixed.flatMap(_.splits).count(_ eq authorized) == 1)
    assert(mixed.flatMap(_.splits).count(_ eq merge) == 1)

    val evolution =
      BinPackingSplits(CoreOptions.fromMap(Map("data-evolution.enabled" -> "true").asJava))
    val groups = evolution.packForParallelism(Array(raw, merge), 8)
    assert(groups.size == 2)
    assert(groups.flatMap(_.splits).exists(_ eq raw))
    assert(groups.flatMap(_.splits).exists(_ eq merge))
  }

  test("scan.parallelism can increase or decrease batch input partitions") {
    withTable("t") {
      sql("CREATE TABLE t (id INT) TBLPROPERTIES ('write-only'='true')")
      (1 to 6).foreach(id => sql(s"INSERT INTO t VALUES ($id)"))

      def scan() = getPaimonScan("SELECT id FROM t")
      withSparkSQLConf("spark.sql.files.minPartitionNum" -> "1") {
        assert(scan().inputPartitions.size == 1)
        sql("ALTER TABLE t SET TBLPROPERTIES ('scan.parallelism'='5')")
        assert(scan().inputPartitions.size == 5)
        withSparkSQLConf("spark.paimon.scan.parallelism" -> "4") {
          val planned = scan()
          assert(planned.inputPartitions.size == 4)
          withSparkSQLConf(s"spark.paimon.paimon.$dbName0.t.scan.parallelism" -> "3") {
            assert(scan().inputPartitions.size == 3)
            assert(planned.copy().inputPartitions.size == 4)
            val path = loadTable("t").location().toString
            assert(spark.read.format("paimon").load(path).rdd.getNumPartitions == 4)
            assert(
              spark.read
                .format("paimon")
                .option("catalog", "paimon")
                .load(path)
                .rdd
                .getNumPartitions == 3)
            val df = spark.read.option("scan.parallelism", "2").table("t")
            assert(df.rdd.getNumPartitions == 2)
            assert(df.collect().map(_.getInt(0)).sorted.toList == (1 to 6).toList)
          }
        }
      }
      val incremental = spark.read
        .option("incremental-between", "1,6")
        .option("scan.parallelism", "4")
        .table("t")
      val incrementalScan = incremental.queryExecution.optimizedPlan.collectFirst {
        case r: DataSourceV2ScanRelation => r.scan.asInstanceOf[PaimonScan]
      }.get
      assert(incrementalScan.inputPartitions.size == 4)
      assert(incrementalScan.inputPartitions.flatMap(_.splits).forall {
        split => !split.asInstanceOf[DataSplit].isStreaming
      })
      assert(incremental.collect().map(_.getInt(0)).sorted.toSeq == (2 to 6))

      val left = spark.read.option("scan.parallelism", "2").table("t")
      val right = spark.read.option("scan.parallelism", "3").table("t")
      val joined = left.join(right, Seq("id"))
      val counts = joined.queryExecution.optimizedPlan.collect {
        case r: DataSourceV2ScanRelation => r.scan.asInstanceOf[PaimonScan].inputPartitions.size
      }
      assert(counts.sorted == Seq(2, 3))
      assert(joined.collect().map(_.getInt(0)).sorted.toSeq == (1 to 6))
      for (invalid <- Seq("0", "-1", "abc", "2147483648")) {
        withSparkSQLConf("spark.paimon.scan.parallelism" -> invalid) {
          val error = intercept[IllegalArgumentException](scan().inputPartitions)
          assert(
            error.getMessage.contains("scan.parallelism") && error.getMessage.contains(invalid))
        }
      }
      withSparkSQLConf(
        "spark.sql.files.minPartitionNum" -> "8",
        "spark.paimon.scan.parallelism" -> "2") {
        assert(scan().inputPartitions.size == 2)
      }
    }
  }

  test("fixed-count packing keeps incremental primary-key updates for the same key") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT, v INT) TBLPROPERTIES
            |('primary-key'='id', 'bucket'='1', 'write-only'='true')""".stripMargin)
      (1 to 6).foreach(i => sql(s"INSERT INTO t VALUES (1, $i)"))
      val df = spark.read
        .option("incremental-between", "1,6")
        .option("incremental-between-scan-mode", "delta")
        .option("scan.parallelism", "4")
        .table("t")
      val scan = df.queryExecution.optimizedPlan.collectFirst {
        case r: DataSourceV2ScanRelation => r.scan.asInstanceOf[PaimonScan]
      }.get
      val splits = scan.inputSplits
      assert(splits.length == 1 && splits.head.asInstanceOf[DataSplit].isStreaming)
      assert(scan.inputPartitions.flatMap(_.splits) == splits.toSeq)
      assert(df.collect().map(_.getInt(1)).sorted.toSeq == (2 to 6))
    }
  }

  test("scan.parallelism leaves micro-batch offsets and input splits unchanged") {
    withTable("t") {
      sql("""CREATE TABLE t (a INT, b INT) PARTITIONED BY (a)
            |TBLPROPERTIES ('write-only'='true')""".stripMargin)
      sql("INSERT INTO t VALUES (1, 0), (2, 0), (3, 0)")
      def planStream() = {
        val stream = getPaimonScan("SELECT * FROM t")
          .toMicroBatchStream("unused")
          .asInstanceOf[PaimonMicroBatchStream]
        try {
          val start = stream.initialOffset()
          val end = stream.latestOffset(start, ReadLimit.allAvailable())
          val splits = stream.planInputPartitions(start, end).toList.map {
            _.asInstanceOf[PaimonInputPartition].splits
          }
          (start.json(), end.json(), splits)
        } finally {
          stream.stop()
        }
      }
      val baseline = planStream()
      assert(baseline._3.size == 3)
      withSparkSQLConf("spark.paimon.scan.parallelism" -> "1") {
        assert(getPaimonScan("SELECT * FROM t").inputPartitions.size == 1)
        assert(planStream() == baseline)
        assert(getPaimonScan("SELECT * FROM t WHERE a = 4").inputPartitions.isEmpty)
      }
    }
  }

  test("Paimon: set open-file-cost to 0") {
    withTable("t") {
      sql("CREATE TABLE t (a INT, b STRING)")
      for (i <- 1 to 100) {
        sql(s"INSERT INTO t VALUES ($i, 'a')")
      }

      def paimonScan() = getPaimonScan("SELECT * FROM t")

      // default openCostInBytes is 4m, so we will get 400 / 128 = 4 partitions
      withSparkSQLConf("spark.sql.files.minPartitionNum" -> "1") {
        assert(paimonScan().inputPartitions.length == 4)
      }

      withSparkSQLConf(
        "spark.sql.files.minPartitionNum" -> "1",
        "spark.sql.files.openCostInBytes" -> "0") {
        assert(paimonScan().inputPartitions.length == 1)
      }

      // Paimon's conf takes precedence over Spark's
      withSparkSQLConf(
        "spark.sql.files.minPartitionNum" -> "1",
        "spark.sql.files.openCostInBytes" -> "4194304",
        "spark.paimon.source.split.open-file-cost" -> "0") {
        assert(paimonScan().inputPartitions.length == 1)
      }
    }
  }

  test("Paimon: bucketed packing preserves independent split groups and bucket keys") {
    withSparkSQLConf("spark.sql.files.minPartitionNum" -> "1") {
      val splits = Seq(0, 0, 0, 1, 1).zipWithIndex.map {
        case (bucket, i) =>
          newDataSplitFromFiles(
            Seq(newDataFile(s"bucket-$bucket-$i.parquet", 60L)),
            rawConvertible = false,
            bucket = bucket)
      }
      val packing = BinPackingSplits(CoreOptions.fromMap(
        Map("source.split.target-size" -> "130 B", "source.split.open-file-cost" -> "0 B").asJava))
      val partitions = packing.packByBucket(splits.toArray)
      assert(partitions.map(_.bucket) == Seq(0, 0, 1))
      assert(partitions.map(_.splits.size) == Seq(2, 1, 2))
      partitions.foreach {
        partition =>
          assert(partition.splits.forall(_.asInstanceOf[DataSplit].bucket() == partition.bucket))
      }
      partitions.flatMap(_.splits).zip(splits).foreach {
        case (actual, original) => Assertions.assertSame(original, actual)
      }
    }
  }

  test("Paimon: bucketed packing keeps oversized merge splits and deletion files intact") {
    val split = newDataSplit("merge", Seq(100L, 100L), deletionFileLength = Some(10L))
    val packing = BinPackingSplits(
      CoreOptions.fromMap(
        Map(
          "deletion-vectors.enabled" -> "true",
          "source.split.target-size" -> "50 B",
          "source.split.open-file-cost" -> "0 B").asJava))
    val partitions = packing.packByBucket(Array(split))
    assert(partitions.size == 1)
    Assertions.assertSame(split, partitions.head.splits.head)
    assert(split.dataFiles().size() == 2)
    assert(split.deletionFiles().get().size() == 2)
    assert(packing.packByBucket(Array.empty[DataSplit]).isEmpty)
  }

  test("Paimon: get read splits with column pruning") {
    withTable("t") {
      sql(
        "CREATE TABLE t (a INT, s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING, s9 STRING)")
      sql(
        "INSERT INTO t SELECT  /*+ REPARTITION(10) */ id, uuid(), uuid(), uuid(), uuid(), uuid(), uuid(), uuid(), uuid(), uuid() FROM range(1000000)")

      withSparkSQLConf(
        "spark.sql.files.minPartitionNum" -> "1",
        "spark.sql.files.openCostInBytes" -> "0",
        "spark.paimon.source.split.target-size" -> "32m") {
        for (splitSizeWithColumnPruning <- Seq("true", "false")) {
          withSparkSQLConf(
            "spark.paimon.source.split.target-size-with-column-pruning" -> splitSizeWithColumnPruning) {
            // Select one col
            var partitionCount = getPaimonScan("SELECT s1 FROM t").inputPartitions.length
            if (splitSizeWithColumnPruning.toBoolean) {
              assert(partitionCount == 1)
            } else {
              assert(partitionCount > 1)
            }

            // Select all cols
            partitionCount = getPaimonScan("SELECT * FROM t").inputPartitions.length
            assert(partitionCount > 1)
          }
        }
      }
    }
  }

  private def newDataSplit(
      prefix: String,
      fileSizes: Seq[Long],
      rawConvertible: Boolean = false,
      deletionFileLength: Option[Long] = None): DataSplit = {
    val files = fileSizes.zipWithIndex.map {
      case (fileSize, index) => newDataFile(s"$prefix-$index.parquet", fileSize)
    }
    newDataSplitFromFiles(files, rawConvertible, deletionFileLength, prefix)
  }

  private def newDataSplitFromFiles(
      files: Seq[DataFileMeta],
      rawConvertible: Boolean,
      deletionFileLength: Option[Long] = None,
      deletionFilePrefix: String = "delete",
      bucket: Int = 0): DataSplit = {
    val builder = DataSplit
      .builder()
      .withSnapshot(1)
      .withBucket(bucket)
      .withPartition(BinaryRow.EMPTY_ROW)
      .withDataFiles(files.asJava)
      .rawConvertible(rawConvertible)
      .withBucketPath("no use")
    deletionFileLength.foreach {
      length =>
        builder.withDataDeletionFiles(
          files.indices
            .map(index => new DeletionFile(s"$deletionFilePrefix-$index.dv", 0, length, null))
            .asJava)
    }
    builder.build()
  }

  private def newDataFile(
      fileName: String,
      fileSize: Long,
      rowCount: Long = 1,
      maxSequenceNumber: Long = 0): DataFileMeta = {
    DataFileMeta.forAppend(
      fileName,
      fileSize,
      rowCount,
      null,
      0,
      maxSequenceNumber,
      1,
      new java.util.ArrayList[String](),
      null,
      FileSource.APPEND,
      null,
      null,
      null,
      null)
  }
}
