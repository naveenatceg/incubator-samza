package org.apache.samza.storage


import java.io.File

import org.apache.samza.container.SamzaContainerContext
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.storage.kv.{KeyValueStore, BaseKeyValueStorageEngineFactory}
import org.apache.samza.system.SystemStreamPartition


/**
 * Created by nsomasun on 8/8/14.
 */
class RocksDbKeyValueStorageEngineFactory [K, V] extends BaseKeyValueStorageEngineFactory[K, V]
{
  /**
   * Return a KeyValueStore instance for the given store name
   * @param storeName Name of the store
   * @param storeDir The directory of the store
   * @param registry MetricsRegistry to which to publish store specific metrics.
   * @param changeLogSystemStreamPartition Samza stream partition from which to receive the changelog.
   * @param containerContext Information about the container in which the task is executing.
   * @return A valid KeyValueStore instance
   */
  override def getKVStore(storeName: String,
                          storeDir: File,
                          registry: MetricsRegistry,
                          changeLogSystemStreamPartition: SystemStreamPartition,
                          containerContext: SamzaContainerContext): KeyValueStore[Array[Byte], Array[Byte]] = {

  }
}
