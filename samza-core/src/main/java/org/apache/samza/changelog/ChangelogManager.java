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


/**
 * The Changelog manager is used to persist and read the changelog information from the coordinator stream.
 */
public class ChangelogManager {
  CoordinatorStreamSystemProducer coordinatorStreamProducer;
  CoordinatorStreamSystemConsumer coordinatorStreamConsumer;
  Set<CoordinatorStreamMessage> bootstrappedStream;
  HashSet<TaskName> taskNames = new HashSet<TaskName>();
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
    log.debug("Adding taskName {} to {}", taskName, this);
    taskNames.add(taskName);
    coordinatorStreamConsumer.register();
    coordinatorStreamProducer.register(taskName.getTaskName());
  }

  /**
   * The bootstrap method is used catchup with the latest version of the coordinator stream's contents.
   * The method also filters for changelog messages.
   */
  private void bootstrapCoordinatorStream() {
    log.debug("Bootstrapping for changelog messages");
    coordinatorStreamConsumer.bootstrap();
    bootstrappedStream = new HashSet<CoordinatorStreamMessage>();

    for (CoordinatorStreamMessage coordinatorStreamMessage : coordinatorStreamConsumer.getBoostrappedStream()) {
      if(coordinatorStreamMessage.getType().equalsIgnoreCase(CoordinatorStreamMessage.SetChangelogMapping.TYPE)) {
        bootstrappedStream.add(coordinatorStreamMessage);
      }
    }
  }

  /**
   * Read the taskName to partition mapping that is being maintained by this ChangelogManager
   * @return TaskName to task log partition mapping, or an empty map if there were no messages.
   */
  public Map<TaskName, Integer> readChangeLogPartitionMapping() {
    log.debug("Reading changelog partition information");
    bootstrapCoordinatorStream();
    HashMap<TaskName, Integer> changelogMapping = new HashMap<TaskName, Integer>();
    for (CoordinatorStreamMessage coordinatorStreamMessage : bootstrappedStream) {
      CoordinatorStreamMessage.SetChangelogMapping changelogMapEntry =
          new CoordinatorStreamMessage.SetChangelogMapping(coordinatorStreamMessage);
      changelogMapping.put(new TaskName(changelogMapEntry.getTaskName()), changelogMapEntry.getPartition());
      log.debug("TaskName: {} is mapped to {}", changelogMapEntry.getTaskName(), changelogMapEntry.getPartition());
    }
    return changelogMapping;
  }

  /**
   * Write the taskName to partition mapping that is being maintained by this ChangelogManager
   * @param changelogEntry The entry that needs to be written to the coordinator stream, the map takes the taskName
   *                       and it's corresponding changelog partition.
   */
  public void writeChangeLogPartitionMapping(Map<TaskName, Integer> changelogEntry) {
    log.debug("Updating changelog information with: ");
    for (Map.Entry<TaskName, Integer> entry : changelogEntry.entrySet()) {
      log.debug("TaskName: {} to Partition: {}", entry.getKey().getTaskName(), entry.getValue());
      CoordinatorStreamMessage.SetChangelogMapping changelogMapping =
          new CoordinatorStreamMessage.SetChangelogMapping(entry.getKey().getTaskName(),
          entry.getKey().getTaskName(),
          entry.getValue());
      coordinatorStreamProducer.send(changelogMapping);
    }
  }

}
