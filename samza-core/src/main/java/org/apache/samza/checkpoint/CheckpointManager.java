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

package org.apache.samza.checkpoint;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.changelog.ChangelogManager;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.stream.CoordinatorStreamMessage;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemConsumer;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CheckpointManager {

    Map<TaskName, Checkpoint> taskNamesToOffsets = new HashMap<TaskName, Checkpoint>();
    CoordinatorStreamSystemProducer coordinatorStreamProducer;
    CoordinatorStreamSystemConsumer coordinatorStreamConsumer;
    Set<CoordinatorStreamMessage> bootstrappedStream;
    HashSet<TaskName> taskNames = new HashSet<TaskName>();
    private static final Logger log = LoggerFactory.getLogger(ChangelogManager.class);

    public CheckpointManager(CoordinatorStreamSystemProducer coordinatorStreamProducer, CoordinatorStreamSystemConsumer coordinatorStreamConsumer) {
    this.coordinatorStreamConsumer = coordinatorStreamConsumer;
    this.coordinatorStreamProducer = coordinatorStreamProducer;
    }


    public void start() {
        coordinatorStreamProducer.start();
        coordinatorStreamConsumer.start();
        bootstrapCoordinatorStream();
    }

    public void bootstrapCoordinatorStream() {
      coordinatorStreamConsumer.bootstrap();
      bootstrappedStream = coordinatorStreamConsumer.getBoostrappedStream();
      HashSet<CoordinatorStreamMessage> filteredSet = new HashSet<CoordinatorStreamMessage>();

      for (CoordinatorStreamMessage coordinatorStreamMessage : bootstrappedStream) {
        if(coordinatorStreamMessage.getType().equalsIgnoreCase(CoordinatorStreamMessage.SetCheckpoint.TYPE))
          filteredSet.add(coordinatorStreamMessage);
      }
      bootstrappedStream = filteredSet;
    }

    /**
     * Registers this manager to write checkpoints of a specific Samza stream partition.
     * @param taskName Specific Samza taskName of which to write checkpoints for.
     */
    public void register(TaskName taskName) {
        log.debug("Adding taskName " + taskName + " to " + this);
        taskNames.add(taskName);
        coordinatorStreamConsumer.register();
        coordinatorStreamProducer.register(taskName.getTaskName());
    }

    /**
     * Writes a checkpoint based on the current state of a Samza stream partition.
     * @param taskName Specific Samza taskName of which to write a checkpoint of.
     * @param checkpoint Reference to a Checkpoint object to store offset data in.
     */
    public void writeCheckpoint(TaskName taskName, Checkpoint checkpoint) {
        CoordinatorStreamMessage.SetCheckpoint checkPointMessage = new CoordinatorStreamMessage.SetCheckpoint(taskName.getTaskName(), taskName.getTaskName(), checkpoint);
        coordinatorStreamProducer.send(checkPointMessage);
    }

    /**
     * Returns the last recorded checkpoint for a specified taskName.
     * @param taskName Specific Samza taskName for which to get the last checkpoint of.
     * @return A Checkpoint object with the recorded offset data of the specified partition.
     */
    public Checkpoint readLastCheckpoint(TaskName taskName) {
        //Bootstrap each time to make sure that we are caught up with the stream, the bootstrap will just catch up on consecutive calls
        bootstrapCoordinatorStream();
      for (CoordinatorStreamMessage coordinatorStreamMessage : bootstrappedStream) {
        CoordinatorStreamMessage.SetCheckpoint setCheckpoint = new CoordinatorStreamMessage.SetCheckpoint(coordinatorStreamMessage);
        TaskName taskNameInCheckpoint = new TaskName(setCheckpoint.getKey());
        if(taskNames.contains(taskNameInCheckpoint)) {
          taskNamesToOffsets.put(taskNameInCheckpoint, setCheckpoint.getCheckpoint());
          log.debug("Adding checkpoint " + taskNameInCheckpoint + " for taskName " + taskName);
        }
      }

        return taskNamesToOffsets.get(taskName);
    }

    public void stop() {
        coordinatorStreamConsumer.stop();
        coordinatorStreamProducer.stop();
    }
}
