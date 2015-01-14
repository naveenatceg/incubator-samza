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

package org.apache.samza.util

import java.net.URL
import java.io.BufferedReader
import java.lang.management.ManagementFactory
import java.io.File
import org.apache.samza.system.SystemStream
import java.util.Random
import org.apache.samza.job.model.JobModel
import java.io.InputStreamReader
import org.apache.samza.config.Config
import org.apache.samza.config.SystemConfig
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config.ConfigException
import org.apache.samza.config.MapConfig
import scala.collection.JavaConversions._
import org.apache.samza.config.JobConfig

object Util extends Logging {
  val random = new Random

  /**
   * Make an environment variable string safe to pass.
   */
  def envVarEscape(str: String) = str.replace("\"", "\\\"").replace("'", "\\'")

  /**
   * Get a random number >= startInclusive, and < endExclusive.
   */
  def randomBetween(startInclusive: Int, endExclusive: Int) =
    startInclusive + random.nextInt(endExclusive - startInclusive)

  /**
   * Recursively remove a directory (or file), and all sub-directories. Equivalent
   * to rm -rf.
   */
  def rm(file: File) {
    if (file == null) {
      return
    } else if (file.isDirectory) {
      val files = file.listFiles()
      if (files != null) {
        for (f <- files)
          rm(f)
      }
      file.delete()
    } else {
      file.delete()
    }
  }

  /**
   * Instantiate a class instance from a given className.
   */
  def getObj[T](className: String) = {
    Class
      .forName(className)
      .newInstance
      .asInstanceOf[T]
  }

  /**
   * Returns a SystemStream object based on the system stream name given. For
   * example, kafka.topic would return new SystemStream("kafka", "topic").
   */
  def getSystemStreamFromNames(systemStreamNames: String): SystemStream = {
    val idx = systemStreamNames.indexOf('.')
    if (idx < 0) {
      throw new IllegalArgumentException("No '.' in stream name '" + systemStreamNames + "'. Stream names should be in the form 'system.stream'")
    }
    new SystemStream(systemStreamNames.substring(0, idx), systemStreamNames.substring(idx + 1, systemStreamNames.length))
  }

  /**
   * Returns a SystemStream object based on the system stream name given. For
   * example, kafka.topic would return new SystemStream("kafka", "topic").
   */
  def getNameFromSystemStream(systemStream: SystemStream) = {
    systemStream.getSystem + "." + systemStream.getStream
  }

  /**
   * Makes sure that an object is not null, and throws a NullPointerException
   * if it is.
   */
  def notNull[T](obj: T, msg: String) = if (obj == null) {
    throw new NullPointerException(msg)
  }

  /**
   * Returns the name representing the JVM. It usually contains the PID of the process plus some additional information
   * @return String that contains the name representing this JVM
   */
  def getContainerPID(): String = {
    ManagementFactory.getRuntimeMXBean().getName()
  }

  /**
   * Reads a URL and returns its body as a string. Does no error handling.
   *
   * @param url HTTP URL to read from.
   * @param timeout How long to wait before timing out when connecting to or reading from the HTTP server.
   * @return String payload of the body of the HTTP response.
   */
  def read(url: URL, timeout: Int = 30000): String = {
    val conn = url.openConnection();
    conn.setConnectTimeout(timeout)
    conn.setReadTimeout(timeout)
    val br = new BufferedReader(new InputStreamReader(conn.getInputStream));
    var line: String = null;
    val body = Iterator.continually(br.readLine()).takeWhile(_ != null).mkString
    br.close
    body
  }

  /**
   * Generates a coordinator stream name based off of the job name and job id
   * for the jobd. The format is of the stream name will be
   * __samza_coordinator_&lt;JOBNAME&gt;_&lt;JOBID&gt;.
   */
  def getCoordinatorStreamName(jobName: String, jobId: String) = {
    "__samza_coordinator_%s_%s" format (jobName.replaceAll("_", "-"), jobId.replaceAll("_", "-"))
  }

  /**
   * Get a job's name and ID given a config. Job ID is defaulted to 1 if not
   * defined in the config, and job name must be defined in config.
   *
   * @return A tuple of (jobName, jobId)
   */
  def getJobNameAndId(config: Config) = {
    (config.getName.getOrElse(throw new ConfigException("Missing required config: job.name")), config.getJobId.getOrElse("1"))
  }

  /**
   * Given a job's full config object, build a subset config which includes
   * only the job name, job id, and system config for the coordinator stream.
   */
  def buildCoordinatorStreamConfig(config: Config) = {
    val (jobName, jobId) = getJobNameAndId(config)
    // Build a map with just the system config and job.name/job.id. This is what's required to start the JobCoordinator.
    new MapConfig(config.subset(SystemConfig.SYSTEM_PREFIX format config.getCoordinatorSystemName, false) ++
      Map[String, String](JobConfig.JOB_NAME -> jobName, JobConfig.JOB_ID -> jobId))
  }
}
