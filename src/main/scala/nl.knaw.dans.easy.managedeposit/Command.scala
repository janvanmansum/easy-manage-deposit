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

import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import java.nio.file.Paths
import scala.annotation.tailrec
import scala.io.StdIn
import scala.language.reflectiveCalls
import scala.util.{ Failure, Try }

object Command extends App with DebugEnhancedLogging {
  type FeedBackMessage = String

  val configuration = Configuration(Paths.get(System.getProperty("app.home")))
  val commandLine: CommandLineOptions = new CommandLineOptions(args, configuration.version)
  val app = new EasyManageDepositApp(configuration)

  @tailrec
  private def cleanInteraction(): Boolean = {
    StdIn.readLine("This action will delete data from the deposit area. OK? (y/n): ") match {
      case "y" => true
      case "n" => false
      case _ =>
        println("Please enter a valid char : y or n")
        cleanInteraction()
    }
  }

  val result: Try[FeedBackMessage] = commandLine.subcommands match {
    case commandLine.reportCmd :: (full @ commandLine.reportCmd.fullCmd) :: Nil =>
      app.createFullReport(full.depositor.toOption, full.datamanager.toOption, full.age.toOption)
    case commandLine.reportCmd :: (summary @ commandLine.reportCmd.summaryCmd) :: Nil =>
      app.summary(summary.depositor.toOption, summary.datamanager.toOption, summary.age.toOption)
    case commandLine.reportCmd :: (error @ commandLine.reportCmd.errorCmd) :: Nil =>
      app.createErrorReport(error.depositor.toOption, error.datamanager.toOption, error.age.toOption)
    case commandLine.reportCmd :: (raw @ commandLine.reportCmd.rawCmd) :: Nil =>
      app.createRawReport(raw.location())
    case (clean @ commandLine.cleanCmd) :: Nil =>
      val newState = for {
        state <- clean.newStateLabel.toOption
        description <- clean.newStateDescription.toOption
      } yield (state, description)
      val deleteParams = DeleteParameters(clean.depositor.toOption, clean.keep(), clean.state(), clean.dataOnly(), clean.doUpdate(), newState, clean.output())
      val deleting = if (clean.doUpdate()) "Deleting"
                     else "To be deleted"
      val dataFrom = if (clean.dataOnly()) "data from "
                     else ""
      val replacingWithState = clean.newStateLabel.toOption.fold("")(stateLabel => s", replacing with state $stateLabel")
      val depositor = clean.depositor.toOption.getOrElse("all users")
      Console.err.println(s"$deleting ${ dataFrom }deposits with state ${ clean.state() }$replacingWithState for $depositor")
      if (clean.force() || cleanInteraction())
        app.cleanDeposits(deleteParams)
      else
        Try { "Clean operation aborted by user" }
    case (syncFedora @ commandLine.`syncFedoraState`) :: Nil =>
      app.syncFedoraState(syncFedora.easyDatasetId())
    case commandLine.runService :: Nil => runAsService(app)
    case _ => Failure(new IllegalArgumentException("Enter a valid subcommand"))
  }

  result.doIfSuccess(msg => Console.err.println(s"OK: $msg"))
    .doIfFailure { case t =>
      Console.err.println(s"ERROR: ${ t.getClass.getSimpleName }: ${ t.getMessage }")
      logger.error("A fatal exception occurred", t)
      System.exit(1)
    }

  private def runAsService(app: EasyManageDepositApp): Try[FeedBackMessage] = Try {
    val service = new EasyManageDepositService(configuration.serverPort, Map(
      "/" -> new EasyManageDepositServlet(app, configuration.version),
    ))
    Runtime.getRuntime.addShutdownHook(new Thread("service-shutdown") {
      override def run(): Unit = {
        service.stop()
        service.destroy()
      }
    })

    service.start()
    Thread.currentThread.join()
    "Service terminated normally."
  }

}

