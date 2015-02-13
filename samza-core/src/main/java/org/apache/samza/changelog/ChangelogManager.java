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

package org.apache.samza.changelog;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.stream.CoordinatorStreamMessage;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemConsumer;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ChangelogManager {
  CoordinatorStreamSystemProducer coordinatorStreamProducer;
  CoordinatorStreamSystemConsumer coordinatorStreamConsumer;
  Set<CoordinatorStreamMessage> bootstrappedStream;
  HashSet<TaskName> taskNames = new HashSet<TaskName>();
  boolean coordinatorStreamConsumerRegistered = false;
  private static final Logger log = LoggerFactory.getLogger(ChangelogManager.class);

  public ChangelogManager(CoordinatorStreamSystemProducer coordinatorStreamProducer, CoordinatorStreamSystemConsumer coordinatorStreamConsumer) {
    this.coordinatorStreamConsumer = coordinatorStreamConsumer;
    this.coordinatorStreamProducer = coordinatorStreamProducer;
  }

  public void start() {
    coordinatorStreamProducer.start();
    coordinatorStreamConsumer.start();
    bootstrapCoordinatorStream();
  }

  public void stop() {
    coordinatorStreamConsumer.stop();
    coordinatorStreamProducer.stop();
  }

  /**
   * Registers this manager to write checkpoints of a specific Samza stream partition.
   * @param taskName Specific Samza taskName of which to write checkpoints for.
   */
  public void register(TaskName taskName) {
    log.debug("Adding taskName " + taskName + " to " + this);
    taskNames.add(taskName);
    if(coordinatorStreamConsumerRegistered) {
      coordinatorStreamConsumer.register();
      coordinatorStreamConsumerRegistered = true;
    }
      coordinatorStreamProducer.register(taskName.getTaskName());
  }


  public void bootstrapCoordinatorStream() {
    coordinatorStreamConsumer.bootstrap();
    bootstrappedStream = new HashSet<CoordinatorStreamMessage>();

    for (CoordinatorStreamMessage coordinatorStreamMessage : coordinatorStreamConsumer.getBoostrappedStream()) {
      if(coordinatorStreamMessage.getType().equals(CoordinatorStreamMessage.SetChangelogMapping.TYPE))
        bootstrappedStream.add(coordinatorStreamMessage);
    }
  }

  /**
   * Read the taskName to partition mapping that is being maintained by this CheckpointManager
   *
   * @return TaskName to task log partition mapping, or an empty map if there were no messages.
   */
  public Map<TaskName, Integer> readChangeLogPartitionMapping() {
    bootstrapCoordinatorStream();
    HashMap<TaskName, Integer> changelogMapping = new HashMap<TaskName, Integer>();
    for (CoordinatorStreamMessage coordinatorStreamMessage : bootstrappedStream) {
      CoordinatorStreamMessage.SetChangelogMapping changelogMapEntry = new CoordinatorStreamMessage.SetChangelogMapping(coordinatorStreamMessage);
      changelogMapping.put(new TaskName(changelogMapEntry.getTaskName()), changelogMapEntry.getPartition());
    }
    return changelogMapping;
  }

  /**
   * Write the taskName to partition mapping that is being maintained by this CheckpointManager
   */
  public void writeChangeLogPartitionMapping(Map<TaskName, Integer> changelogEntry) {
    for (Map.Entry<TaskName, Integer> entry : changelogEntry.entrySet()) {
      CoordinatorStreamMessage.SetChangelogMapping changelogMapping = new
          CoordinatorStreamMessage.SetChangelogMapping("ChangelogManager",
          entry.getKey().getTaskName(),
          entry.getValue());
      coordinatorStreamProducer.send(changelogMapping);
    }
  }

}
