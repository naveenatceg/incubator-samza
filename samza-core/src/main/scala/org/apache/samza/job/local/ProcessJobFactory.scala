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

package org.apache.samza.job.local

import org.apache.samza.container.SamzaContainer
import org.apache.samza.util.Logging
import org.apache.samza.SamzaException
import org.apache.samza.config.Config
import org.apache.samza.config.TaskConfig._
import org.apache.samza.job.{ CommandBuilder, ShellCommandBuilder, StreamJob, StreamJobFactory }
import org.apache.samza.util.Util
import scala.collection.JavaConversions._
import org.apache.samza.coordinator.server.HttpServer
import org.apache.samza.coordinator.server.JobServlet
import org.apache.samza.coordinator.JobCoordinator

/**
 * Creates a stand alone ProcessJob with the specified config.
 */
class ProcessJobFactory extends StreamJobFactory with Logging {
  def getJob(config: Config): StreamJob = {
    val coordinator = JobCoordinator(config)
    val containerModel = coordinator.jobModel.getContainers.get(0)

    try {
      val commandBuilder = {
        config.getCommandClass match {
          case Some(cmdBuilderClassName) => {
            // A command class was specified, so we need to use a process job to
            // execute the command in its own process.
            Util.getObj[CommandBuilder](cmdBuilderClassName)
          }
          case _ => {
            info("Defaulting to ShellCommandBuilder")
            new ShellCommandBuilder
          }
        }
      }

      val coordinatorSystemConfig = Util.buildCoordinatorStreamConfig(config)

      commandBuilder
        .setConfig(coordinatorSystemConfig)
        .setId(0)

      coordinator.start

      new ProcessJob(commandBuilder)
    } finally {
      coordinator.stop
    }
  }
}
