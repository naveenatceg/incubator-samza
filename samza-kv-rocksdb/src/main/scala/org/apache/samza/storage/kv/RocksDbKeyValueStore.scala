package org.apache.samza.storage.kv


import java.io.File

import grizzled.slf4j.{Logging, Logger}
import org.apache.samza.config.Config
import org.apache.samza.container.SamzaContainerContext
import org.rocksdb._;


/**
 * Created by nsomasun on 8/8/14.
 */
object RocksDbKeyValueStore
{
  private lazy val logger = Logger(classOf[RocksDbKeyValueStore])
  def options(storeConfig: Config, containerContext: SamzaContainerContext) = {
    val cacheSize = storeConfig.getLong("container.cache.size.bytes", 100 * 1024 * 1024L)
    val writeBufSize = storeConfig.getLong("container.write.buffer.size.bytes", 32 * 1024 * 1024)
    val options = new Options();

    // Cache size and write buffer size are specified on a per-container basis.
    options.setCacheSize(cacheSize / containerContext.partitions.size)
    options.setWriteBufferSize((writeBufSize / containerContext.partitions.size).toInt)
    options.setBlockSize(storeConfig.getInt("rocksdb.block.size.bytes", 4096))
    options.setCompressionType(
      storeConfig.get("rocksdb.compression", "snappy") match {
        case "snappy" => CompressionType.SNAPPY_COMPRESSION
        case "none" => CompressionType.NO_COMPRESSION
        case _ =>
          logger.warn("Unknown rocksdb.compression codec %s, defaulting to Snappy" format storeConfig.get("rocksdb.compression", "snappy"))
          CompressionType.SNAPPY_COMPRESSION
      })
    options.setCreateIfMissing(true)
    options.setErrorIfExists(true)
    options
  }

}

class RocksDbKeyValueStore(

  val dir: File,
  val options: Options,
  /**
  * How many deletes must occur before we will force a compaction. This is to
  * get around performance issues discovered in SAMZA-254. A value of -1
  * disables this feature.
  */
  val deleteCompactionThreshold: Int = -1,
  val metrics: KeyValueStoreMetrics = new KeyValueStoreMetrics) extends KeyValueStore[Array[Byte], Array[Byte]] with Logging {

  private lazy val db = RocksDB.open(options,dir.toString)
  private val lexicographic = new LexicographicComparator()
  private var deletesSinceLastCompaction = 0


  def get(key: Array[Byte]): Array[Byte] = {
    maybeCompact
    metrics.gets.inc
    require(key != null, "Null key not allowed.")
    val found = db.get(key)
    if (found != null) {
      metrics.bytesRead.inc(found.size)
    }
    found
  }

  def put(key: Array[Byte], value: Array[Byte]) {
    metrics.puts.inc
    require(key != null, "Null key not allowed.")
    if (value == null) {
      db.remove(key)
      deletesSinceLastCompaction += 1
    } else {
      metrics.bytesWritten.inc(key.size + value.size)
      db.put(key, value)
    }
  }

  def putAll(entries: java.util.List[Entry[Array[Byte], Array[Byte]]]) {
    val batch = new WriteBatch()
    val iter = entries.iterator
    var wrote = 0
    var deletes = 0
    while (iter.hasNext) {
      wrote += 1
      val curr = iter.next()
      if (curr.getValue == null) {
        deletes += 1
        batch.remove(curr.getKey)
      } else {
        val key = curr.getKey
        val value = curr.getValue
        metrics.bytesWritten.inc(key.size + value.size)
        batch.put(key, value)
      }
    }
    db.write(new WriteOptions(), batch)
    metrics.puts.inc(wrote)
    metrics.deletes.inc(deletes)
    deletesSinceLastCompaction += deletes
  }

  def delete(key: Array[Byte]) {
    metrics.deletes.inc
    put(key, null)
  }

  def range(from: Array[Byte], to: Array[Byte]): KeyValueIterator[Array[Byte], Array[Byte]] = {
    maybeCompact
    metrics.ranges.inc
    require(from != null && to != null, "Null bound not allowed.")
    new RocksDbRangeIterator(db.newIterator(),from,to)
  }

  def all(): KeyValueIterator[Array[Byte], Array[Byte]] = {
    maybeCompact
    metrics.alls.inc
    val iter = db.newIterator()
    iter.seekToFirst()
    new RocksDbIterator(iter)
  }

  /**
   * Trigger a complete compaction on the LevelDB store if there have been at
   * least deleteCompactionThreshold deletes since the last compaction.
   */
  def maybeCompact = {
    if (deleteCompactionThreshold >= 0 && deletesSinceLastCompaction >= deleteCompactionThreshold) {
      compact
    }
  }

  /**
   * Trigger a complete compaction of the LevelDB store.
   */
  def compact {
    // According to LevelDB's docs:
    // begin==NULL is treated as a key before all keys in the database.
    // end==NULL is treated as a key after all keys in the database.
    //db.compactRange(null, null)
    //deletesSinceLastCompaction = 0
  }

  def flush {
    metrics.flushes.inc
    // TODO can't find a flush for leveldb
    trace("Flushing, but flush in RocksDbKeyValueStore doesn't do anything.")
  }

  def close() {
    trace("Closing.")

    db.close()
  }


  class RocksDbIterator(iter: RocksIterator) extends KeyValueIterator[Array[Byte], Array[Byte]] {
    private var open = true
    def close() = {
      open = false
      //iter.close()
    }

    def remove() =  throw new UnsupportedOperationException("RocksDB iterator doesn't support remove"); //iter.remove();

    def hasNext() = {
      iter.next()
      val hasNext = iter.isValid
      iter.prev()
      hasNext
    };

    protected def peekKey() = {
      iter.next()
      val key = iter.key()
      iter.prev()
      key
    }

    def next() = {
      if (!hasNext()) {
        throw new NoSuchElementException
      }

      iter.next
      val key = iter.key
      val value = iter.value
      metrics.bytesRead.inc(key.size)
      if (value != null) {
        metrics.bytesRead.inc(value.size)
      }
      new Entry(key, value)
    }

    override def finalize() {
      if (open) {
        System.err.println("Leaked reference to level db iterator, forcing close.")
        close()
      }
    }
  }

  class RocksDbRangeIterator(iter: RocksIterator, from: Array[Byte], to: Array[Byte]) extends RocksDbIterator(iter) {
    val comparator = lexicographic //if (options.comparator == null) lexicographic else options.comparator
    iter.seek(from)
    override def hasNext() = {
      iter.isValid && comparator.compare(peekKey, to) < 0
    }
  }

  /**
   * Compare two array lexicographically using unsigned byte arithmetic
   */
  class LexicographicComparator {
    def compare(k1: Array[Byte], k2: Array[Byte]): Int = {
      val l = math.min(k1.length, k2.length)
      var i = 0
      while (i < l) {
        if (k1(i) != k2(i))
          return (k1(i) & 0xff) - (k2(i) & 0xff)
        i += 1
      }
      // okay prefixes are equal, the shorter array is less
      k1.length - k2.length
    }
    def name(): String = "lexicographic"
    def findShortestSeparator(start: Array[Byte], limit: Array[Byte]) = start
    def findShortSuccessor(key: Array[Byte]) = key
  }

}