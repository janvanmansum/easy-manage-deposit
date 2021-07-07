/**
 * Copyright (C) 2017 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.managedeposit

import java.nio.file.Path

import nl.knaw.dans.easy.managedeposit.State.State
import org.rogach.scallop.{ ScallopConf, ScallopOption, Subcommand, ValueConverter, singleArgConverter }

import scala.language.{ postfixOps, reflectiveCalls }

class CommandLineOptions(args: Array[String], configuration: Configuration) extends ScallopConf(args) {
  appendDefaultToDescription = true
  editBuilder(_.setHelpWidth(110))
  printedName = "easy-manage-deposit"
  private val SUBCOMMAND_SEPARATOR = "---\n"
  val description: String = s"""Manage DANS deposit directories."""
  val synopsis: String =
    s"""
       |  $printedName report full [-a, --age <n>] [-m, --datamanager <uid>] [<depositor>]
       |  $printedName report summary [-a, --age <n>] [-m, --datamanager <uid>] [<depositor>]
       |  $printedName report error [-a, --age <n>] [-m, --datamanager <uid>] [<depositor>]
       |  $printedName report raw [<location>]
       |  $printedName clean [-d, --data-only] [-s, --state <state>] [-k, --keep <n>]  \\
       |            [-l, --new-state-label <state>] [-n, --new-state-description <description>] \\
       |            [-f, --force] [-o, --output] [--do-update] [<depositor>]
       |  $printedName sync-fedora-state <easy-dataset-id>
     """.stripMargin
  version(s"$printedName v${ configuration.version }")
  banner(
    s"""
       |  $description
       |
       |Usage:
       |
       |$synopsis
       |
       |Options:
       |
       |""".stripMargin)
  private implicit val stateParser: ValueConverter[State] = singleArgConverter(State.withName)

  val reportCmd = new Subcommand("report") {

    val fullCmd = new Subcommand("full") {
      val depositor: ScallopOption[DepositorId] = trailArg("depositor", required = false)
      val datamanager: ScallopOption[Datamanager] = opt("datamanager", short = 'm',
        descr = "Only report on the deposits that are assigned to this datamanager.")
      val age: ScallopOption[Age] = opt[Age](name = "age", short = 'a', validate = 0 <=,
        descr = "Only report on the deposits that are less than n days old. An age argument of n=0 days corresponds to 0<=n<1. If this argument is not provided, all deposits will be reported on.")
      descr("creates a full report for a depositor and/or datamanager")
      footer(SUBCOMMAND_SEPARATOR)
    }
    addSubcommand(fullCmd)

    val summaryCmd = new Subcommand("summary") {
      val depositor: ScallopOption[DepositorId] = trailArg("depositor", required = false)
      val datamanager: ScallopOption[Datamanager] = opt("datamanager", short = 'm',
        descr = "Only report on the deposits that are assigned to this datamanager.")
      val age: ScallopOption[Age] = opt[Age](name = "age", short = 'a', validate = 0 <=,
        descr = "Only report on the deposits that are less than n days old. An age argument of n=0 days corresponds to 0<=n<1. If this argument is not provided, all deposits will be reported on.")
      descr("creates a summary report for a depositor and/or datamanager")
      footer(SUBCOMMAND_SEPARATOR)
    }
    addSubcommand(summaryCmd)

    val errorCmd = new Subcommand("error") {
      val depositor: ScallopOption[DepositorId] = trailArg("depositor", required = false)
      val datamanager: ScallopOption[Datamanager] = opt("datamanager", short = 'm',
        descr = "Only report on the deposits that are assigned to this datamanager.")
      val age: ScallopOption[Age] = opt[Age](name = "age", short = 'a', validate = 0 <=,
        descr = "Only report on the deposits that are less than n days old. An age argument of n=0 days corresponds to 0<=n<1. If this argument is not provided, all deposits will be reported on.")
      descr("creates a report displaying all failed, rejected and invalid deposits for a depositor and/or datamanager")
      footer(SUBCOMMAND_SEPARATOR)
    }
    addSubcommand(errorCmd)
    
    val rawCmd = new Subcommand("raw") {
      val location: ScallopOption[Path] = trailArg[Path](name = "location")
      
      validatePathExists(location)
      validatePathIsDirectory(location)
      
      descr("creates a report containing all content of deposit.properties without inferring any properties")
      footer(SUBCOMMAND_SEPARATOR)
    }
    addSubcommand(rawCmd)
  }
  addSubcommand(reportCmd)

  val cleanCmd = new Subcommand("clean") {
    val depositor: ScallopOption[DepositorId] = trailArg("depositor", required = false)
    val dataOnly: ScallopOption[Boolean] = opt[Boolean](default = Some(false), descr = "If specified, the deposit.properties and the container file of the deposit are not deleted")
    val state: ScallopOption[State] = opt[State](required= true, descr = "The deposits with the specified state argument are deleted")
    val keep: ScallopOption[Int] = opt[Int](default = Some(-1), validate = -1 <=, descr = "The deposits whose ages are greater than or equal to the argument n (days) are deleted. An age argument of n=0 days corresponds to 0<=n<1.")
    val newStateLabel: ScallopOption[State] = opt(short = 'l', descr = "The state label in deposit.properties after the deposit has been deleted")
    val newStateDescription: ScallopOption[String] = opt[String](short = 'n', descr = "The state description in deposit.properties after the deposit has been deleted")
    val force: ScallopOption[Boolean] = opt[Boolean](default = Some(false), descr = "The user is not asked for a confirmation")
    val output: ScallopOption[Boolean] = opt[Boolean](default = Some(false), descr = "Output a list of depositIds of the deposits that were deleted")
    val doUpdate: ScallopOption[Boolean] = opt[Boolean](noshort = true, default = Some(false), descr = "Do the actual deleting of deposits and updating of deposit.properties")

    // newStateLabel and newStateDescription can only be given together (both of them of neither of them has to be present)
    // newStateLabel and newStateDescription can only be given when also dataOnly is given
    codependent(newStateLabel, newStateDescription)
    dependsOnAll(newStateLabel, List(dataOnly, newStateDescription))

    descr("removes deposit with specified state")
    footer(SUBCOMMAND_SEPARATOR)
  }
  addSubcommand(cleanCmd)

  val syncFedoraState = new Subcommand("sync-fedora-state") {
    val easyDatasetId: ScallopOption[DatasetId] = trailArg("easy-dataset-id", descr = "The dataset identifier of the deposit which deposit.properties are being synced with Fedora")
    descr("Syncs a deposit with Fedora, checks if the deposit is properly registered in Fedora and updates the deposit.properties accordingly")
    footer(SUBCOMMAND_SEPARATOR)
  }
  addSubcommand(syncFedoraState)

  footer("")
  verify()
}
