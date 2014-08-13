package org.apache.samza.storage.kv
import org.rocksdb._;
import org.rocksdb.util.SizeUnit;

object TestRocksDbApi
{

  def main(args : Array[String])
  {
    System.out.println("RocksDBSample");
    val options = new Options();
    var db = RocksDB.open(options, "/tmp/testdbpath");
    options.setCompressionType(CompressionType.LZ4HC_COMPRESSION).setCreateIfMissing(true);
    val filter = new BloomFilter(10);
    options.setCreateIfMissing(true).createStatistics().setWriteBufferSize(8 * SizeUnit.KB).setMaxWriteBufferNumber(3).setDisableSeekCompaction(
      true).setBlockSize(64 * SizeUnit.KB).setMaxBackgroundCompactions(10).setFilter(filter);
    val stats = options.statisticsPtr();

    assert(options.createIfMissing() == true);
    assert(options.writeBufferSize() == 8 * SizeUnit.KB);
    assert(options.maxWriteBufferNumber() == 3);
    assert(options.disableSeekCompaction() == true);
    assert(options.blockSize() == 64 * SizeUnit.KB);
    assert(options.maxBackgroundCompactions() == 10);

    assert(options.memTableFactoryName().equals("SkipListFactory"));
    options.setMemTableConfig(new HashSkipListMemTableConfig().setHeight(4).setBranchingFactor(4).setBucketCount(2000000));
    assert(options.memTableFactoryName().equals("HashSkipListRepFactory"));

    options.setMemTableConfig(new HashLinkedListMemTableConfig().setBucketCount(100000));
    assert(options.memTableFactoryName().equals("HashLinkedListRepFactory"));

    options.setMemTableConfig(new VectorMemTableConfig().setReservedSize(10000));
    assert(options.memTableFactoryName().equals("VectorRepFactory"));

    options.setMemTableConfig(new SkipListMemTableConfig());
    assert(options.memTableFactoryName().equals("SkipListFactory"));

    options.setTableFormatConfig(new PlainTableConfig());
    assert(options.tableFactoryName().equals("PlainTable"));
    db.put("d".getBytes(), "wahevers".getBytes());
    db.put("hello".getBytes(), "world".getBytes());
    db.put("c".getBytes(),"whatevers".getBytes());
    db.put("a".getBytes(), "whatever".getBytes());
    db.put("b".getBytes(), "wahevers".getBytes());
    db.put("e".getBytes(), "wahevers".getBytes());
    db.put("aa".getBytes(), "what".getBytes());
    val value = db.get("hello".getBytes());
    assert("world".equals(new String(value)));

    val it = db.newIterator();
    it.seek("c".getBytes());
    while(it.isValid)
    {
      System.out.println(new String(it.key));
      it.next
    }

    // be sure to release the c++ pointer
    db.close();
  }
}


