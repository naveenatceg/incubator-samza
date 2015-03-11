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


import java.util.Collections

import org.apache.samza.changelog.ChangelogManager
import org.apache.samza.config.Config
import org.apache.samza.job.model.JobModel
import org.apache.samza.SamzaException
import org.apache.samza.container.grouper.task.GroupByContainerCount
import org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouperFactory
import java.util
import org.apache.samza.container.TaskName
import org.apache.samza.util.Logging
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.util.Util
import scala.collection.JavaConversions._
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.Partition
import org.apache.samza.job.model.TaskModel
import org.apache.samza.system.StreamMetadataCache
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.system.SystemFactory
import org.apache.samza.coordinator.server.HttpServer
import org.apache.samza.checkpoint.{Checkpoint, CheckpointManager}
import org.apache.samza.coordinator.server.JobServlet
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.coordinator.stream.{CoordinatorStreamSystemConsumer, CoordinatorStreamSystemProducer, CoordinatorStreamMessage, CoordinatorStreamSystemFactory}
import org.apache.samza.config.ConfigRewriter

/**
 * Helper companion object that is responsible for wiring up a JobCoordinator
 * given a Config object.
 */
object JobCoordinator extends Logging {
  
  var coordinatorSystemConsumer: CoordinatorStreamSystemConsumer = null
  var coordinatorSystemProducer: CoordinatorStreamSystemProducer = null
  
  /**
   * @param coordinatorSystemConfig A config object that contains job.name,
   * job.id, and all system.&lt;job-coordinator-system-name&gt;.*
   * configuration. The method will use this config to read all configuration
   * from the coordinator stream, and instantiate a JobCoordinator.
   */
  def apply(coordinatorSystemConfig: Config) = {
    coordinatorSystemConsumer = new CoordinatorStreamSystemFactory().getCoordinatorStreamSystemConsumer(coordinatorSystemConfig, new MetricsRegistryMap)
    coordinatorSystemProducer = new CoordinatorStreamSystemFactory().getCoordinatorStreamSystemProducer(coordinatorSystemConfig, new MetricsRegistryMap)
    info("Registering coordinator system stream.")
    coordinatorSystemConsumer.register
    debug("Starting coordinator system stream.")
    coordinatorSystemConsumer.start
    debug("Bootstrapping coordinator system stream.")
    coordinatorSystemConsumer.bootstrap
    debug("Stopping coordinator system stream.")
    coordinatorSystemConsumer.stop
    val config = coordinatorSystemConsumer.getConfig
    info("Got config: %s" format config)
    getJobCoordinator(rewriteConfig(config))
  }

  /**
   * Build a JobCoordinator using a Samza job's configuration.
   */
  def getJobCoordinator(config: Config) = {
    val containerCount = config.getContainerCount
    def jobModelGenerator(): JobModel = buildJobModel(config, containerCount)
    val server = new HttpServer
    server.addServlet("/*", new JobServlet(jobModelGenerator))
    new JobCoordinator(jobModelGenerator(), server)
  }

  /**
   * Gets a CheckpointManager from the configuration.
   */
  def getCheckpointManager(config: Config) = {
    new CheckpointManager(coordinatorSystemProducer, coordinatorSystemConsumer)
  }

  def getChangelogManager(config: Config) = {
    new ChangelogManager(coordinatorSystemProducer, coordinatorSystemConsumer)
  }

  /**
   * For each input stream specified in config, exactly determine its
   * partitions, returning a set of SystemStreamPartitions containing them all.
   */
  def getInputStreamPartitions(config: Config) = {
    val inputSystemStreams = config.getInputStreams
    val systemNames = config.getSystemNames.toSet

    // Map the name of each system to the corresponding SystemAdmin
    val systemAdmins = systemNames.map(systemName => {
      val systemFactoryClassName = config
        .getSystemFactory(systemName)
        .getOrElse(throw new SamzaException("A stream uses system %s, which is missing from the configuration." format systemName))
      val systemFactory = Util.getObj[SystemFactory](systemFactoryClassName)
      systemName -> systemFactory.getAdmin(systemName, config)
    }).toMap

    // Get the set of partitions for each SystemStream from the stream metadata
    new StreamMetadataCache(systemAdmins)
      .getStreamMetadata(inputSystemStreams)
      .flatMap {
        case (systemStream, metadata) =>
          metadata
            .getSystemStreamPartitionMetadata
            .keys
            .map(new SystemStreamPartition(systemStream, _))
      }.toSet
  }

  /**
   * Gets a SystemStreamPartitionGrouper object from the configuration.
   */
  def getSystemStreamPartitionGrouper(config: Config) = {
    val factoryString = config.getSystemStreamPartitionGrouperFactory
    val factory = Util.getObj[SystemStreamPartitionGrouperFactory](factoryString)
    factory.getSystemStreamPartitionGrouper(config)
  }

  /**
   * Re-writes configuration using a ConfigRewriter, if one is defined. If
   * there is no ConfigRewriter defined for the job, then this method is a
   * no-op.
   *
   * @param config The config to re-write.
   */
  def rewriteConfig(config: Config): Config = {
    def rewrite(c: Config, rewriterName: String): Config = {
      val klass = config
        .getConfigRewriterClass(rewriterName)
        .getOrElse(throw new SamzaException("Unable to find class config for config rewriter %s." format rewriterName))
      val rewriter = Util.getObj[ConfigRewriter](klass)
      info("Re-writing config with " + rewriter)
      rewriter.rewrite(rewriterName, c)
    }

    config.getConfigRewriters match {
      case Some(rewriters) => rewriters.split(",").foldLeft(config)(rewrite(_, _))
      case _ => config
    }
  }

  /**
   * Build a full Samza job model using the job configuration.
   */
  def buildJobModel(config: Config, containerCount: Int) = {
    // TODO containerCount should go away when we generalize the job coordinator, 
    // and have a non-yarn-specific way of specifying container count.
    val changelogManager = getChangelogManager(config)
    val allSystemStreamPartitions = getInputStreamPartitions(config)
    val grouper = getSystemStreamPartitionGrouper(config)
    val previousChangelogeMapping = if (changelogManager != null) {
      changelogManager.start
      changelogManager.readChangeLogPartitionMapping
    } else {
      new util.HashMap[TaskName, java.lang.Integer]()
    }

    // If no mappings are present(first time the job is running) we return -1, this will allow 0 to be the first change
    // mapping.
    var maxChangelogPartitionId = previousChangelogeMapping
      .values
      .map(_.toInt)
      .toList
      .sorted
      .lastOption
      .getOrElse(-1)

    val checkpointManager = getCheckpointManager(config)
    // Assign all SystemStreamPartitions to TaskNames.
    val taskModels = {
      val groups = grouper.group(allSystemStreamPartitions)
      info("SystemStreamPartitionGrouper " + grouper + " has grouped the SystemStreamPartitions into the following taskNames:")
      groups
        .map {
          case (taskName, systemStreamPartitions) =>
            checkpointManager.register(taskName)
            val checkpoint = Option(checkpointManager.readLastCheckpoint(taskName)).getOrElse(new Checkpoint(new util.HashMap[SystemStreamPartition, String]()))
            var offsetMap = new util.HashMap[SystemStreamPartition, String]()
            offsetMap.putAll(checkpoint.getOffsets)
            // Find the system partitions which don't have a checkpoint and set null for the values for offsets
            (systemStreamPartitions -- offsetMap.keySet()).foreach(offsetMap += _ -> null)

            val changelogPartition = Option(previousChangelogeMapping.get(taskName)) match {
                case Some(changelogPartitionId) => new Partition(changelogPartitionId)
              case _ =>
                // If we've never seen this TaskName before, then assign it a 
                // new changelog.
                maxChangelogPartitionId += 1
                info("New task %s is being assigned changelog partition %s." format (taskName, maxChangelogPartitionId))
                new Partition(maxChangelogPartitionId)
            }
            new TaskModel(taskName, offsetMap, changelogPartition)
        }
        .toSet
    }

    // Save the changelog mapping back to the changelog manager
    if (changelogManager != null) {
      // newChangelogMapping is the merging of all current task:changelog
      // assignments with whatever we had before (previousChangelogeMapping).
      // We must persist legacy changelog assignments so that 
      // maxChangelogPartitionId always has the absolute max, not the current 
      // max (in case the task with the highest changelog partition mapping 
      // disappears.
      val newChangelogMapping = taskModels.map(taskModel => {
        taskModel.getTaskName -> Integer.valueOf(taskModel.getChangelogPartition.getPartitionId)
      }).toMap ++ previousChangelogeMapping
      info("Saving task-to-changelog partition mapping: %s" format newChangelogMapping)
      changelogManager.writeChangeLogPartitionMapping(newChangelogMapping)
      changelogManager.stop
    }

    // Here is where we should put in a pluggable option for the 
    // SSPTaskNameGrouper for locality, load-balancing, etc.
    val containerGrouper = new GroupByContainerCount(containerCount)
    val containerModels = containerGrouper
      .group(taskModels)
      .map { case (containerModel) => Integer.valueOf(containerModel.getContainerId) -> containerModel }
      .toMap

    new JobModel(config, containerModels)
  }
}

/**
 * <p>JobCoordinator is responsible for managing the lifecycle of a Samza job
 * once it's been started. This includes starting and stopping containers,
 * managing configuration, etc.</p>
 *
 * <p>Any new cluster manager that's integrated with Samza (YARN, Mesos, etc)
 * must integrate with the job coordinator.</p>
 *
 * <p>This class' API is currently unstable, and likely to change. The
 * coordinator's responsibility is simply to propagate the job model, and HTTP
 * server right now.</p>
 */
class JobCoordinator(
  /**
   * The data model that describes the Samza job's containers and tasks.
   */
  val jobModel: JobModel,

  /**
   * HTTP server used to serve a Samza job's container model to SamzaContainers when they start up.
   */
  val server: HttpServer = null) extends Logging {

  debug("Got job model: %s." format jobModel)

  def start {
    if (server != null) {
      debug("Starting HTTP server.")
      server.start
      info("Startd HTTP server: %s" format server.getUrl)
    }
  }

  def stop {
    if (server != null) {
      debug("Stopping HTTP server.")
      server.stop
      info("Stopped HTTP server.")
    }
  }
}