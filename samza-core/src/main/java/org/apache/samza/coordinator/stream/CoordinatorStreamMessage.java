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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Represents a message for the job coordinator. All messages in the coordinator
 * stream must wrap the CoordinatorStreamMessage class. Coordinator stream
 * messages are modeled as key/value pairs. The key is a list of well defined
 * fields: version, type, and key. The value is a map. There are some
 * pre-defined fields (such as timestamp, host, etc) for the value map, which
 * are common to all messages.
 * </p>
 * 
 * <p>
 * The full structure for a CoordinatorStreamMessage is:
 * </p>
 * 
 * <pre>
 * key =&gt; [1, "set-config", "job.name"] 
 * 
 * message =&gt; {
 *   "host": "192.168.0.1",
 *   "username": "criccomini",
 *   "source": "job-runner",
 *   "timestamp": 123456789,
 *   "values": {
 *     "value": "my-job-name"
 *   } 
 * }
 * </pre>
 * 
 * Where the key's structure is:
 * 
 * <pre>
 * key =&gt; [&lt;version&gt;, &lt;type&gt;, &lt;key&gt;]
 * </pre>
 * 
 * <p>
 * Note that the white space in the above JSON blobs are done for legibility.
 * Over the wire, the JSON should be compact, and no unnecessary white space
 * should be used. This is extremely important for key serialization, since a
 * key with [1,"set-config","job.name"] and [1, "set-config", "job.name"] will
 * be evaluated as two different keys, and Kafka will not log compact them (if
 * Kafka is used as the underlying system for a coordinator stream).
 * </p>
 * 
 * <p>
 * The "values" map in the message is defined on a per-message-type basis. For
 * set-config messages, there is just a single key/value pair, where the "value"
 * key is defined. For offset messages, there will be multiple key/values pairs
 * in "values" (one for each SystemStreamPartition/offset pair for a given
 * TaskName).
 * </p>
 * 
 * <p>
 * The most important fields are type, key, and values. The type field (defined
 * as index 1 in the key list) defines the kind of message, the key (defined as
 * index 2 in the key list) defines a key to associate with the values, and the
 * values map defines a set of values associated with the type. A concrete
 * example would be a config message of type "set-config" with key "job.name"
 * and values {"value": "my-job-name"}.
 * </p>
 */
public class CoordinatorStreamMessage {
  public static int VERSION_INDEX = 0;
  public static int TYPE_INDEX = 1;
  public static int KEY_INDEX = 2;

  private static final Logger log = LoggerFactory.getLogger(CoordinatorStreamMessage.class);

  /**
   * Protocol version for coordinator stream messages. This version number must
   * be incremented any time new messages are added to the coordinator stream,
   * or changes are made to the key/message headers.
   */
  public static final int VERSION = 1;

  /**
   * Contains all key fields. Currently, this includes the type of the message,
   * the key associated with the type (e.g. type: set-config key: job.name), and
   * the version of the protocol. The indices are defined as the INDEX static
   * variables above.
   */
  private final Object[] keyArray;

  /**
   * Contains all fields for the message. This includes who sent the message,
   * the host, etc. It also includes a "values" map, which contains all values
   * associated with the key of the message. If set-config/job.name were used as
   * the type/key of the message, then values would contain
   * {"value":"my-job-name"}.
   */
  private final Map<String, Object> messageMap;
  private boolean isDelete;

  public CoordinatorStreamMessage(CoordinatorStreamMessage message) {
    this(message.getKeyArray(), message.getMessageMap());
  }

  public CoordinatorStreamMessage(Object[] keyArray, Map<String, Object> messageMap) {
    this.keyArray = keyArray;
    this.messageMap = messageMap;
    this.isDelete = messageMap == null;
  }

  public CoordinatorStreamMessage(String source) {
    this(source, new Object[] { Integer.valueOf(VERSION), null, null }, new HashMap<String, Object>());
  }

  public CoordinatorStreamMessage(String source, Object[] keyArray, Map<String, Object> messageMap) {
    this(keyArray, messageMap);
    if (!isDelete) {
      this.messageMap.put("values", new HashMap<String, String>());
      setSource(source);
      setUsername(System.getProperty("user.name"));
      setTimestamp(System.currentTimeMillis());

      try {
        setHost(InetAddress.getLocalHost().getHostAddress());
      } catch (UnknownHostException e) {
        log.warn("Unable to retrieve host for current machine. Setting coordinator stream message host field to an empty string.");
        setHost("");
      }
    }

    setVersion(VERSION);
  }

  protected void setIsDelete(boolean isDelete) {
    this.isDelete = isDelete;
  }

  protected void setHost(String host) {
    messageMap.put("host", host);
  }

  protected void setUsername(String username) {
    messageMap.put("username", username);
  }

  protected void setSource(String source) {
    messageMap.put("source", source);
  }

  protected void setTimestamp(long timestamp) {
    messageMap.put("timestamp", Long.valueOf(timestamp));
  }

  protected void setVersion(int version) {
    this.keyArray[VERSION_INDEX] = Integer.valueOf(version);
  }

  protected void setType(String type) {
    this.keyArray[TYPE_INDEX] = type;
  }

  protected void setKey(String key) {
    this.keyArray[KEY_INDEX] = key;
  }

  @SuppressWarnings("unchecked")
  protected Map<String, String> getMessageValues() {
    return (Map<String, String>) this.messageMap.get("values");
  }

  protected String getMessageValue(String key) {
    return getMessageValues().get(key);
  }

  protected void putMessageValue(String key, String value) {
    getMessageValues().put(key, value);
  }

  /**
   * The type of the message is used to convert a generic
   * CoordinatorStreaMessage into a specific message, such as a SetConfig
   * message.
   * 
   * @return The type of the message.
   */
  public String getType() {
    return (String) this.keyArray[TYPE_INDEX];
  }

  /**
   * @return The whole key map including both the key and type of the message.
   */
  public Object[] getKeyArray() {
    return this.keyArray;
  }

  /**
   * @return Whether the message signifies a delete or not.
   */
  public boolean isDelete() {
    return isDelete;
  }

  /**
   * @return Whether the message signifies a delete or not.
   */
  public String getUsername() {
    return (String) this.messageMap.get("username");
  }

  /**
   * @return Whether the message signifies a delete or not.
   */
  public long getTimestamp() {
    return (Long) this.messageMap.get("timestamp");
  }

  /**
   * @return The whole message map including header information.
   */
  public Map<String, Object> getMessageMap() {
    if (!isDelete) {
      Map<String, Object> immutableMap = new HashMap<String, Object>(messageMap);
      immutableMap.put("values", Collections.unmodifiableMap(getMessageValues()));
      return Collections.unmodifiableMap(immutableMap);
    } else {
      return null;
    }
  }

  /**
   * @return The source that sent the coordinator message. This is a string
   *         defined by the sender.
   */
  public String getSource() {
    return (String) this.messageMap.get("source");
  }

  /**
   * @return The protocol version that the message conforms to.
   */
  public int getVersion() {
    return (Integer) this.keyArray[VERSION_INDEX];
  }

  /**
   * @return The key for a message. The key's meaning is defined by the type of
   *         the message.
   */
  public String getKey() {
    return (String) this.keyArray[KEY_INDEX];
  }

  @Override
  public String toString() {
    return "CoordinatorStreamMessage [keyArray=" + Arrays.toString(keyArray) + ", messageMap=" + messageMap + ", isDelete=" + isDelete + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (isDelete ? 1231 : 1237);
    result = prime * result + Arrays.hashCode(keyArray);
    result = prime * result + ((messageMap == null) ? 0 : messageMap.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    CoordinatorStreamMessage other = (CoordinatorStreamMessage) obj;
    if (isDelete != other.isDelete)
      return false;
    if (!Arrays.equals(keyArray, other.keyArray))
      return false;
    if (messageMap == null) {
      if (other.messageMap != null)
        return false;
    } else if (!messageMap.equals(other.messageMap))
      return false;
    return true;
  }

  /**
   * A coordinator stream message that tells the job coordinator to set a
   * specific configuration.
   */
  public static class SetConfig extends CoordinatorStreamMessage {
    public static final String TYPE = "set-config";

    public SetConfig(CoordinatorStreamMessage message) {
      super(message.getKeyArray(), message.getMessageMap());
    }

    public SetConfig(String source, String key, String value) {
      super(source);
      setType(TYPE);
      setKey(key);
      putMessageValue("value", value);
    }

    public String getConfigValue() {
      return (String) getMessageValue("value");
    }
  }

  public static class Delete extends CoordinatorStreamMessage {
    public Delete(String source, String key, String type) {
      this(source, key, type, VERSION);
    }

    /**
     * <p>
     * Delete messages must take the type of another CoordinatorStreamMessage
     * (e.g. SetConfig) to define the type of message that's being deleted.
     * Considering Kafka's log compaction, for example, the keys of a message
     * and its delete key must match exactly:
     * </p>
     * 
     * <pre>
     * k=&gt;[1,"job.name","set-config"] .. v=&gt; {..some stuff..}
     * v=&gt;[1,"job.name","set-config"] .. v=&gt; null
     * </pre>
     * 
     * <p>
     * Deletes are modeled as a CoordinatorStreamMessage with a null message
     * map, and a key that's identical to the key map that's to be deleted.
     * </p>
     * 
     * @param source
     *          The source ID of the sender of the delete message.
     * @param key
     *          The key to delete.
     * @param type
     *          The type of message to delete. Must correspond to one of hte
     *          other CoordinatorStreamMessages.
     * @param version
     *          The protocol version.
     */
    public Delete(String source, String key, String type, int version) {
      super(source);
      setType(type);
      setKey(key);
      setVersion(version);
      setIsDelete(true);
    }
  }
}
