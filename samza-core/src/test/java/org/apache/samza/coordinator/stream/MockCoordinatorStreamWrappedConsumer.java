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

package org.apache.samza.coordinator.stream;

import java.util.Map;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.coordinator.stream.CoordinatorStreamMessage.SetConfig;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.BlockingEnvelopeMap;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * A mock SystemConsumer that pretends to be a coordinator stream. The mock will
 * take all configs given to it, and put them into the coordinator stream's
 * SystemStreamPartition. This is useful in cases where config needs to be
 * quickly passed from a unit test into the JobCoordinator.
 */
public class MockCoordinatorStreamWrappedConsumer extends BlockingEnvelopeMap {
  private final static ObjectMapper MAPPER = SamzaObjectMapper.getObjectMapper();

  private final SystemStreamPartition systemStreamPartition;
  private final Config config;

  public MockCoordinatorStreamWrappedConsumer(SystemStreamPartition systemStreamPartition, Config config) {
    super();
    this.config = config;
    this.systemStreamPartition = systemStreamPartition;
  }

  public void start() {
    try {
      for (Map.Entry<String, String> configPair : config.entrySet()) {
        SetConfig setConfig = new SetConfig("source", configPair.getKey(), configPair.getValue());
        byte[] keyBytes = MAPPER.writeValueAsString(setConfig.getKeyArray()).getBytes("UTF-8");
        byte[] messgeBytes = MAPPER.writeValueAsString(setConfig.getMessageMap()).getBytes("UTF-8");
        put(systemStreamPartition, new IncomingMessageEnvelope(systemStreamPartition, "", keyBytes, messgeBytes));
      }
      setIsAtHead(systemStreamPartition, true);
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  public void stop() {
  }
}
