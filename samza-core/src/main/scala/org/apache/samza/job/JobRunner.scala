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

package org.apache.samza.job

import org.apache.samza.SamzaException
import org.apache.samza.config.Config
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.job.ApplicationStatus.Running
import org.apache.samza.util.Util
import org.apache.samza.util.CommandLine
import org.apache.samza.util.Logging
import scala.collection.JavaConversions._
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.config.ConfigException
import org.apache.samza.config.SystemConfig
import org.apache.samza.system.SystemFactory
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.coordinator.stream.CoordinatorStreamMessage
import org.apache.samza.coordinator.stream.CoordinatorStreamMessage.SetConfig
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemProducer
import org.apache.samza.system.SystemStream
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemFactory

object JobRunner {
  val SOURCE = "job-runner"

  def main(args: Array[String]) {
    val cmdline = new CommandLine
    val options = cmdline.parser.parse(args: _*)
    val config = cmdline.loadConfig(options)
    new JobRunner(config).run
  }
}

/**
 * ConfigRunner is a helper class that sets up and executes a Samza job based
 * on a config URI. The configFactory is instantiated, fed the configPath,
 * and returns a Config, which is used to execute the job.
 */
class JobRunner(config: Config) extends Logging {
  def run() = {
    debug("config: %s" format (config))
    val jobFactoryClass = config.getStreamJobFactoryClass match {
      case Some(factoryClass) => factoryClass
      case _ => throw new SamzaException("no job factory class defined")
    }
    val jobFactory = Class.forName(jobFactoryClass).newInstance.asInstanceOf[StreamJobFactory]
    info("job factory: %s" format (jobFactoryClass))
    val factory = new CoordinatorStreamSystemFactory
    val coordinatorSystemConsumer = factory.getCoordinatorStreamSystemConsumer(config, new MetricsRegistryMap)
    val coordinatorSystemProducer = factory.getCoordinatorStreamSystemProducer(config, new MetricsRegistryMap)
    info("Storing config in coordinator stream.")
    coordinatorSystemProducer.register(JobRunner.SOURCE)
    coordinatorSystemProducer.start
    coordinatorSystemProducer.writeConfig(JobRunner.SOURCE, config)
    info("Loading old config from coordinator stream.")
    coordinatorSystemConsumer.register
    coordinatorSystemConsumer.start
    coordinatorSystemConsumer.bootstrap
    coordinatorSystemConsumer.stop
    val oldConfig = coordinatorSystemConsumer.getConfig();
    info("Deleting old configs that are no longer defined: %s".format(oldConfig.keySet -- config.keySet))
    (oldConfig.keySet -- config.keySet).foreach(key => {
      coordinatorSystemProducer.send(new CoordinatorStreamMessage.Delete(JobRunner.SOURCE, key, SetConfig.TYPE))
    })
    coordinatorSystemProducer.stop

    // Create the actual job, and submit it.
    val job = jobFactory.getJob(config).submit

    info("waiting for job to start")

    // Wait until the job has started, then exit.
    Option(job.waitForStatus(Running, 500)) match {
      case Some(appStatus) => {
        if (Running.equals(appStatus)) {
          info("job started successfully")
        } else {
          warn("unable to start job successfully. job has status %s" format (appStatus))
        }
      }
      case _ => warn("unable to start job successfully.")
    }

    info("exiting")
    job
  }
}
