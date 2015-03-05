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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.coordinator.stream.CoordinatorStreamMessage.Delete;
import org.apache.samza.coordinator.stream.CoordinatorStreamMessage.SetConfig;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin;
import org.junit.Test;

public class TestCoordinatorStreamSystemConsumer {
  @Test
  public void testCoordinatorStreamSystemConsumer() {
    Map<String, String> expectedConfig = new HashMap<String, String>();
    expectedConfig.put("job.id", "1234");
    SystemStream systemStream = new SystemStream("system", "stream");
    MockSystemConsumer systemConsumer = new MockSystemConsumer(new SystemStreamPartition(systemStream, new Partition(0)));
    CoordinatorStreamSystemConsumer consumer = new CoordinatorStreamSystemConsumer(systemStream, systemConsumer, new SinglePartitionWithoutOffsetsSystemAdmin());
    assertFalse(systemConsumer.isRegistered());
    consumer.register();
    assertTrue(systemConsumer.isRegistered());
    assertFalse(systemConsumer.isStarted());
    consumer.start();
    assertTrue(systemConsumer.isStarted());
    try {
      consumer.getConfig();
      fail("Should have failed when retrieving config before bootstrapping.");
    } catch (SamzaException e) {
      // Expected.
    }
    consumer.bootstrap();
    assertEquals(expectedConfig, consumer.getConfig());
    assertFalse(systemConsumer.isStopped());
    consumer.stop();
    assertTrue(systemConsumer.isStopped());
  }

  private static class MockSystemConsumer implements SystemConsumer {
    private boolean started = false;
    private boolean stopped = false;
    private boolean registered = false;
    private final SystemStreamPartition expectedSystemStreamPartition;
    private int pollCount = 0;

    public MockSystemConsumer(SystemStreamPartition expectedSystemStreamPartition) {
      this.expectedSystemStreamPartition = expectedSystemStreamPartition;
    }

    public void start() {
      started = true;
    }

    public void stop() {
      stopped = true;
    }

    public void register(SystemStreamPartition systemStreamPartition, String offset) {
      registered = true;
      assertEquals(expectedSystemStreamPartition, systemStreamPartition);
    }

    public boolean isRegistered() {
      return registered;
    }

    public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(Set<SystemStreamPartition> systemStreamPartitions, long timeout) throws InterruptedException {
      Map<SystemStreamPartition, List<IncomingMessageEnvelope>> map = new HashMap<SystemStreamPartition, List<IncomingMessageEnvelope>>();
      assertEquals(1, systemStreamPartitions.size());
      SystemStreamPartition systemStreamPartition = systemStreamPartitions.iterator().next();
      assertEquals(expectedSystemStreamPartition, systemStreamPartition);

      if (pollCount++ == 0) {
        List<IncomingMessageEnvelope> list = new ArrayList<IncomingMessageEnvelope>();
        SetConfig setConfig1 = new SetConfig("test", "job.name", "my-job-name");
        SetConfig setConfig2 = new SetConfig("test", "job.id", "1234");
        Delete delete = new Delete("test", "job.name", SetConfig.TYPE);
        list.add(new IncomingMessageEnvelope(systemStreamPartition, null, serialize(setConfig1.getKeyArray()), serialize(setConfig1.getMessageMap())));
        list.add(new IncomingMessageEnvelope(systemStreamPartition, null, serialize(setConfig2.getKeyArray()), serialize(setConfig2.getMessageMap())));
        list.add(new IncomingMessageEnvelope(systemStreamPartition, null, serialize(delete.getKeyArray()), delete.getMessageMap()));
        map.put(systemStreamPartition, list);
      }

      return map;
    }

    private byte[] serialize(Object obj) {
      try {
        return SamzaObjectMapper.getObjectMapper().writeValueAsString(obj).getBytes("UTF-8");
      } catch (Exception e) {
        throw new SamzaException(e);
      }
    }

    public boolean isStarted() {
      return started;
    }

    public boolean isStopped() {
      return stopped;
    }
  }
}
