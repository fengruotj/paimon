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

package org.apache.paimon.spark.read

import org.apache.paimon.CoreOptions
import org.apache.paimon.CoreOptions._
import org.apache.paimon.io.DataFileMeta
import org.apache.paimon.spark.{PaimonBucketedInputPartition, PaimonInputPartition}
import org.apache.paimon.spark.util.SplitUtils
import org.apache.paimon.table.FallbackReadFileStoreTable.FallbackSplit
import org.apache.paimon.table.format.FormatDataSplit
import org.apache.paimon.table.source.{DataSplit, DeletionFile, Split, Splits}
import org.apache.paimon.utils.BinPacking

import org.apache.spark.internal.Logging
import org.apache.spark.sql.PaimonSparkSession
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.internal.SQLConf

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class BinPackingSplits(coreOptions: CoreOptions, readRowSizeRatio: Double = 1.0)
  extends SQLConfHelper
  with Logging {

  private val spark = PaimonSparkSession.active

  private lazy val deletionVectors: Boolean = coreOptions.deletionVectorsEnabled()

  private lazy val filesMaxPartitionBytes: Long =
    BinPackingSplits.filesMaxPartitionBytes(coreOptions, conf)

  private lazy val openCostInBytes: Long =
    BinPackingSplits.openCostInBytes(coreOptions, conf)

  private lazy val leafNodeDefaultParallelism: Int = {
    conf
      .getConf(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM)
      .getOrElse(spark.sparkContext.defaultParallelism)
  }

  def pack(splits: Array[Split]): Seq[PaimonInputPartition] = {
    val (toReshuffle, reserved) = splits.partition {
      case _: FallbackSplit => false
      case split: DataSplit => split.rawConvertible() || coreOptions.dataEvolutionEnabled()
      // FormatDataSplit is already packed with multiple files by target size in the core scan,
      // so each split maps directly to one input partition here.
      case _: FormatDataSplit => false
      case _ => false
    }
    if (toReshuffle.nonEmpty) {
      val startTS = System.currentTimeMillis()
      val reshuffled = if (coreOptions.dataEvolutionEnabled()) {
        packDataEvolutionSplit(toReshuffle.collect { case ds: DataSplit => ds })
      } else {
        packDataSplit(toReshuffle.collect { case ds: DataSplit => ds })
      }
      val all = reserved.map(PaimonInputPartition.apply) ++ reshuffled
      val duration = System.currentTimeMillis() - startTS
      logInfo(
        s"Reshuffle splits from ${toReshuffle.length} to ${reshuffled.length} in $duration ms. " +
          s"Total number of splits is ${all.length}")
      all
    } else {
      splits.map(PaimonInputPartition.apply)
    }
  }

  /** Assign independently readable files or whole splits to a fixed number of nonempty groups. */
  def packForParallelism(splits: Array[Split], parallelism: Int): Seq[PaimonInputPartition] = {
    require(parallelism > 0, s"scan.parallelism must be positive: $parallelism")
    val units = new ArrayBuffer[BinPackingSplits.ReadUnit]
    splits.indices.foreach {
      index =>
        val split = splits(index)
        Splits.underlying(split) match {
          case ds: DataSplit =>
            val deletions = ds.deletionFiles().orElse(null)
            val deletionIterator =
              if (deletions == null) Iterator.continually(null) else deletions.asScala.iterator
            val files = ds.dataFiles().asScala.iterator.map(file => (file, deletionIterator.next()))
            // A wrapper or subclass can carry additional reader state that must stay with the split.
            if (
              (ds eq split) && ds.getClass == classOf[DataSplit] && ds.rawConvertible() &&
              !coreOptions.dataEvolutionEnabled()
            ) {
              files.zipWithIndex.foreach {
                case ((file, deletion), fileIndex) =>
                  units += BinPackingSplits.ReadUnit(
                    index,
                    fileWeight(file, deletion),
                    fileIndex,
                    file,
                    deletion)
              }
            } else {
              val weight = files.map { case (file, deletion) => fileWeight(file, deletion) }.sum
              units += BinPackingSplits.ReadUnit(index, weight)
            }
          case _ =>
            val weight = SplitUtils.splitSize(split) + SplitUtils.fileCount(split) * openCostInBytes
            units += BinPackingSplits.ReadUnit(index, weight)
        }
    }

    val groups = BinPacking
      .packForFixedBinNumber[BinPackingSplits.ReadUnit](units.asJava, _.packingWeight, parallelism)
    groups.asScala.map {
      group =>
        val packed = group.asScala.groupBy(_.splitIndex).toSeq.sortBy(_._1).map {
          case (index, items) =>
            val original = splits(index)
            if (items.head.fileIndex < 0) {
              original
            } else {
              val ds = original.asInstanceOf[DataSplit]
              if (items.size == ds.dataFiles().size()) {
                ds
              } else {
                val ordered = items.sortBy(_.fileIndex).toList
                copyDataSplit(
                  ds,
                  ordered.map(_.file),
                  if (deletionVectors) ordered.map(_.deletion) else Nil)
              }
            }
        }
        PaimonInputPartition(packed)
    }.toList
  }

  private def fileWeight(file: DataFileMeta, deletion: DeletionFile): Long = {
    (file.fileSize() * readRowSizeRatio).toLong + openCostInBytes +
      Option(deletion).map(_.length()).getOrElse(0L)
  }

  /**
   * Preserve bucket keys without collapsing a whole bucket into one input partition. Spark can
   * group these partitions when a distribution is required, or schedule them separately for a
   * partially clustered join. Keep each DataSplit intact to preserve merge-on-read and
   * data-evolution file groups.
   */
  def packByBucket(splits: Array[DataSplit]): Seq[PaimonBucketedInputPartition] = {
    if (splits.isEmpty) {
      return Seq.empty
    }
    val maxSplitBytes = computeMaxSplitBytes(splits)
    splits.groupBy(_.bucket()).toSeq.sortBy(_._1).flatMap {
      case (bucket, bucketSplits) =>
        packWholeDataSplits(bucketSplits, maxSplitBytes)
          .map(group => PaimonBucketedInputPartition(group, bucket))
    }
  }

  private def packDataSplit(splits: Array[DataSplit]): Array[PaimonInputPartition] = {
    val maxSplitBytes = computeMaxSplitBytes(splits)

    var currentSize = 0L
    val currentSplits = new ArrayBuffer[DataSplit]
    val partitions = new ArrayBuffer[PaimonInputPartition]

    var currentSplit: Option[DataSplit] = None
    val currentDataFiles = new ArrayBuffer[DataFileMeta]
    val currentDeletionFiles = new ArrayBuffer[DeletionFile]

    def closeDataSplit(): Unit = {
      if (currentSplit.nonEmpty && currentDataFiles.nonEmpty) {
        val newSplit =
          copyDataSplit(currentSplit.get, currentDataFiles.toSeq, currentDeletionFiles.toSeq)
        currentSplits += newSplit
      }
      currentDataFiles.clear()
      currentDeletionFiles.clear()
    }

    def closeInputPartition(): Unit = {
      closeDataSplit()
      if (currentSplits.nonEmpty) {
        partitions += PaimonInputPartition(currentSplits.toArray)
      }
      currentSplits.clear()
      currentSize = 0
    }

    splits.foreach {
      split =>
        if (!currentSplit.exists(withSamePartitionAndBucket(_, split))) {
          // close and open another data split
          closeDataSplit()
          currentSplit = Some(split)
        }

        val ddFiles = dataFileAndDeletionFiles(split)
        ddFiles.foreach {
          case (dataFile, deletionFile) =>
            val size = fileWeight(dataFile, deletionFile)
            if (currentSize + size > maxSplitBytes) {
              closeInputPartition()
            }
            currentDataFiles += dataFile
            if (deletionVectors) {
              currentDeletionFiles += deletionFile
            }
            currentSize += size
        }
    }
    closeInputPartition()

    partitions.toArray
  }

  private def packDataEvolutionSplit(splits: Array[DataSplit]): Array[PaimonInputPartition] = {
    packWholeDataSplits(splits, computeMaxSplitBytes(splits))
      .map(group => PaimonInputPartition(group))
      .toArray
  }

  private def packWholeDataSplits(
      splits: Array[DataSplit],
      maxSplitBytes: Long): Seq[Seq[DataSplit]] = {
    var currentSize = 0L
    val currentSplits = new ArrayBuffer[DataSplit]
    val partitions = new ArrayBuffer[Seq[DataSplit]]

    def closeInputPartition(): Unit = {
      if (currentSplits.nonEmpty) {
        partitions += currentSplits.toVector
        currentSplits.clear()
        currentSize = 0L
      }
    }

    splits.foreach {
      split =>
        val ddFiles = dataFileAndDeletionFiles(split)
        val size = ddFiles.map {
          case (dataFile, deletionFile) =>
            fileWeight(dataFile, deletionFile)
        }.sum
        if (currentSplits.nonEmpty && currentSize + size > maxSplitBytes) {
          closeInputPartition()
        }
        currentSplits += split
        currentSize += size
    }

    closeInputPartition()
    partitions.toSeq
  }

  private def copyDataSplit(
      split: DataSplit,
      dataFiles: Seq[DataFileMeta],
      deletionFiles: Seq[DeletionFile]): DataSplit = {
    val builder = DataSplit
      .builder()
      .withSnapshot(split.snapshotId())
      .withPartition(split.partition())
      .withBucket(split.bucket())
      .withTotalBuckets(split.totalBuckets())
      .withDataFiles(dataFiles.toList.asJava)
      .rawConvertible(split.rawConvertible)
      .withBucketPath(split.bucketPath)
    if (deletionVectors) {
      builder.withDataDeletionFiles(deletionFiles.toList.asJava)
    }
    builder.build()
  }

  private def withSamePartitionAndBucket(split1: DataSplit, split2: DataSplit): Boolean = {
    split1.partition().equals(split2.partition()) && split1.bucket() == split2.bucket()
  }

  private def dataFileAndDeletionFiles(split: DataSplit): Array[(DataFileMeta, DeletionFile)] = {
    if (deletionVectors && split.deletionFiles().isPresent) {
      val deletionFiles = split.deletionFiles().get().asScala
      split.dataFiles().asScala.zip(deletionFiles).toArray
    } else {
      split.dataFiles().asScala.map((_, null)).toArray
    }
  }

  private def computeMaxSplitBytes(dataSplits: Seq[DataSplit]): Long = {
    val defaultMaxSplitBytes = filesMaxPartitionBytes
    val minPartitionNum = conf.filesMinPartitionNum.getOrElse(leafNodeDefaultParallelism)

    val totalRawBytes = BinPackingSplits.estimatedSize(dataSplits, openCostInBytes)
    val bytesPerCore = totalRawBytes / minPartitionNum

    val maxSplitBytes = Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
    logInfo(
      s"File open cost in bytes: $openCostInBytes, " +
        s"default max split bytes: $defaultMaxSplitBytes, " +
        s"min partition num: $minPartitionNum, " +
        s"final max split bytes: $maxSplitBytes")

    maxSplitBytes
  }
}

object BinPackingSplits {

  // A negative file index denotes a whole split, including its reader-specific metadata.
  private case class ReadUnit(
      splitIndex: Int,
      weight: Long,
      fileIndex: Int = -1,
      file: DataFileMeta = null,
      deletion: DeletionFile = null) {
    // The Java bin packer calls its boxed weight function on every sort comparison.
    val packingWeight: java.lang.Long = math.max(1L, weight)
  }

  private[spark] def filesMaxPartitionBytes(coreOptions: CoreOptions, conf: SQLConf): Long = {
    val options = coreOptions.toConfiguration
    if (options.containsKey(SOURCE_SPLIT_TARGET_SIZE.key())) {
      options.get(SOURCE_SPLIT_TARGET_SIZE).getBytes
    } else if (conf.contains(SQLConf.FILES_MAX_PARTITION_BYTES.key)) {
      conf.getConf(SQLConf.FILES_MAX_PARTITION_BYTES)
    } else {
      SOURCE_SPLIT_TARGET_SIZE.defaultValue().getBytes
    }
  }

  private[spark] def openCostInBytes(coreOptions: CoreOptions, conf: SQLConf): Long = {
    val options = coreOptions.toConfiguration
    if (options.containsKey(SOURCE_SPLIT_OPEN_FILE_COST.key())) {
      options.get(SOURCE_SPLIT_OPEN_FILE_COST).getBytes
    } else if (conf.contains(SQLConf.FILES_OPEN_COST_IN_BYTES.key)) {
      conf.getConf(SQLConf.FILES_OPEN_COST_IN_BYTES)
    } else {
      SOURCE_SPLIT_OPEN_FILE_COST.defaultValue().getBytes
    }
  }

  private[spark] def estimatedSize(splits: Iterable[Split], openCostInBytes: Long): Long = {
    splits
      .map(split => SplitUtils.splitSize(split) + SplitUtils.fileCount(split) * openCostInBytes)
      .sum
  }
}
