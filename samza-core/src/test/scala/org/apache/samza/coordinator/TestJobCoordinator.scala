/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.coordinator


import org.apache.samza.utilj.UtilJ
import org.junit.Test
import org.junit.Assert._
import scala.collection.JavaConversions._
import org.apache.samza.config.MapConfig
import org.apache.samza.config.TaskConfig
import org.apache.samza.config.SystemConfig
import org.apache.samza.container.TaskName
import org.apache.samza.checkpoint.Checkpoint
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.config.Config
import org.apache.samza.system.SystemFactory
import org.apache.samza.system.SystemAdmin
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.system.SystemStreamMetadata
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.Partition
import org.apache.samza.job.model.JobModel
import org.apache.samza.job.model.ContainerModel
import org.apache.samza.job.model.TaskModel
import org.apache.samza.config.JobConfig
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.system.SystemConsumer
import org.apache.samza.coordinator.stream.{MockCoordinatorStreamWrappedConsumer, MockCoordinatorStreamSystemFactory}

class TestJobCoordinator {
  /**
   * Builds a coordinator from config, and then compares it with what was
   * expected. We simulate having a checkpoint manager that has 2 task
   * changelog entries, and our model adds a third task. Expectation is that
   * the JobCoordinator will assign the new task with a new changelog
   * partition.z``zz
   */
  @Test
  def testJobCoordinator {
    val task0Name = new TaskName("Partition 0")
    val checkpoint0 = Map(new SystemStreamPartition("test", "stream1", new Partition(0)) -> "4")
    val task1Name = new TaskName("Partition 1")
    val checkpoint1 = Map(new SystemStreamPartition("test", "stream1", new Partition(1)) ->  "3")
    val task2Name = new TaskName("Partition 2")
    val checkpoint2 = Map(new SystemStreamPartition("test", "stream1", new Partition(2)) -> null)

    // Construct the expected JobModel, so we can compare it to
    // JobCoordinator's JobModel.
    val container0Tasks = Map(
      task0Name -> new TaskModel(task0Name, checkpoint0, new Partition(4)),
      task2Name -> new TaskModel(task2Name, checkpoint2, new Partition(5)))
    val container1Tasks = Map(
      task1Name -> new TaskModel(task1Name, checkpoint1, new Partition(3)))
    val containers = Map(
      Integer.valueOf(0) -> new ContainerModel(0, container0Tasks),
      Integer.valueOf(1) -> new ContainerModel(1, container1Tasks))


    // The test does not pass offsets for task2 (Partition 2) to the checkpointmanager, this will verify that we get an offset 0 for this partition
    val checkpointOffset0 = MockCoordinatorStreamWrappedConsumer.CHECKPOINTPREFIX + "mock:" +
            task0Name.getTaskName() -> (UtilJ.sspToString(checkpoint0.keySet.iterator.next()) + ":" + checkpoint0.values.iterator.next())
    val checkpointOffset1 = MockCoordinatorStreamWrappedConsumer.CHECKPOINTPREFIX + "mock:" +
            task1Name.getTaskName() -> (UtilJ.sspToString(checkpoint1.keySet.iterator.next()) + ":" + checkpoint1.values.iterator.next())
    val changelogInfo0 = MockCoordinatorStreamWrappedConsumer.CHANGELOGPREFIX + "mock:" + task0Name.getTaskName() -> "4"
    val changelogInfo1 = MockCoordinatorStreamWrappedConsumer.CHANGELOGPREFIX + "mock:" + task1Name.getTaskName() -> "3"
    val changelogInfo2 = MockCoordinatorStreamWrappedConsumer.CHANGELOGPREFIX + "mock:" + task2Name.getTaskName() -> "5"

    // Configs which are processed by the MockCoordinatorStream as special configs which are interpreted as
    // SetCheckpoint and SetChangelog
    val otherConfigs = Map(
      checkpointOffset0,
      checkpointOffset1,
      changelogInfo0,
      changelogInfo1,
      changelogInfo2
    )

    val config = Map(
      JobConfig.JOB_NAME -> "test",
      JobConfig.JOB_COORDINATOR_SYSTEM -> "coordinator",
      JobConfig.JOB_CONTAINER_COUNT -> "2",
      TaskConfig.INPUT_STREAMS -> "test.stream1",
      SystemConfig.SYSTEM_FACTORY.format("test") -> classOf[MockSystemFactory].getCanonicalName,
      SystemConfig.SYSTEM_FACTORY.format("coordinator") -> classOf[MockCoordinatorStreamSystemFactory].getName
      )

    val coordinator = JobCoordinator(new MapConfig(config ++ otherConfigs))
    coordinator.start
    val jobModel = new JobModel(new MapConfig(config), containers)
    assertEquals(new MapConfig(config), coordinator.jobModel.getConfig)
    assertEquals(jobModel, coordinator.jobModel)
  }

  @Test
  def testJobCoordinatorCheckpointing = {
    // Write a couple of checkpoints that the jobcoordinator will process

    // start the job coordinator and verify if it has all the checkpoints through http port

    // Write more checkpoint objects

    // Verify if the coordinator has it

    // Write more checkpoints but block on read on the mock consumer

    // verify if the port times out (might have to be done in another thread)

    // verify if it has read the new checkpoints after timeout expires
  }


}

class MockSystemFactory extends SystemFactory {
  def getConsumer(systemName: String, config: Config, registry: MetricsRegistry) = new SystemConsumer {
    def start() {}
    def stop() {}
    def register(systemStreamPartition: SystemStreamPartition, offset: String) {}
    def poll(systemStreamPartitions: java.util.Set[SystemStreamPartition], timeout: Long) = new java.util.HashMap[SystemStreamPartition, java.util.List[IncomingMessageEnvelope]]()
  }
  def getProducer(systemName: String, config: Config, registry: MetricsRegistry) = null
  def getAdmin(systemName: String, config: Config) = new MockSystemAdmin
}

class MockSystemAdmin extends SystemAdmin {
  def getOffsetsAfter(offsets: java.util.Map[SystemStreamPartition, String]) = null
  def getSystemStreamMetadata(streamNames: java.util.Set[String]): java.util.Map[String, SystemStreamMetadata] = {
    assertEquals(1, streamNames.size)
    val partitionMetadata = Map(
      new Partition(0) -> new SystemStreamPartitionMetadata(null, null, null),
      new Partition(1) -> new SystemStreamPartitionMetadata(null, null, null),
      // Create a new Partition(2), which wasn't in the prior changelog mapping.
      new Partition(2) -> new SystemStreamPartitionMetadata(null, null, null))
    Map(streamNames.toList.head -> new SystemStreamMetadata("foo", partitionMetadata))
  }

  override def createChangelogStream(topicName: String, numOfChangeLogPartitions: Int) {
    new UnsupportedOperationException("Method not implemented.")
  }

  override def createCoordinatorStream(streamName: String) {
    new UnsupportedOperationException("Method not implemented.")
  }
}