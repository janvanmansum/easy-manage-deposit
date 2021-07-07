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

import nl.knaw.dans.easy.managedeposit.Curation.requestChangesDescription
import nl.knaw.dans.easy.managedeposit.State.{ ARCHIVED, FAILED, INVALID, REJECTED, State, UNKNOWN }
import org.apache.commons.csv.CSVFormat
import resource.managed

import java.io.PrintStream
import java.text.SimpleDateFormat
import java.util.Calendar

object ReportGenerator {
  private val KB = 1024L
  private val MB = 1024L * KB
  private val GB = 1024L * MB
  private val TB = 1024L * GB

  def filterDepositsByDepositor(deposits: Deposits, depositor: Option[DepositorId] = None): Deposits = {
    depositor.map(d => deposits.filter(_.depositor == d)).getOrElse(deposits)
  }

  def groupAndSortDepositsAlphabeticallyByState(deposits: Deposits): Seq[(State, Seq[DepositInformation])] = {
    val groupedByState = deposits.groupBy(deposit => Option(deposit.state).getOrElse(UNKNOWN))
    groupedByState.toSeq.sortBy { case (state, _) => state } //sort alphabetically by state
  }

  def outputFullReport(deposits: Deposits)(implicit printStream: PrintStream): Unit = printRecords(deposits)

  def outputErrorReport(deposits: Deposits)(implicit printStream: PrintStream): Unit = {
    printRecords(deposits.filter {
      case DepositInformation(_, _, _, _, _, INVALID, "abandoned draft, data removed", _, _, _, _, _, _, _, _) => false // see `clean-deposits.sh` (clean DRAFT section)
      case DepositInformation(_, _, _, _, _, INVALID, _, _, _, _, _, _, _, _, _) => true
      case DepositInformation(_, _, _, _, _, FAILED, _, _, _, _, _, _, _, _, _) => true
      case DepositInformation(_, _, _, _, _, REJECTED, `requestChangesDescription`, _, _, _, _, "API", _, _, _) => false
      case DepositInformation(_, _, _, _, _, REJECTED, _, _, _, _, _, _, _, _, _) => true
      case DepositInformation(_, _, _, _, _, UNKNOWN, _, _, _, _, _, _, _, _, _) => true
      case DepositInformation(_, _, _, _, _, null, _, _, _, _, _, _, _, _, _) => true
      // When the doi of an archived deposit is NOT registered, an error should be raised
      case d @ DepositInformation(_, _, Some(false), _, _, ARCHIVED, _, _, _, _, _, _, _, _, _) if d.isDansDoi => true
      case _ => false
    })
  }

  def outputRawReport(table: Seq[Seq[String]])(implicit printStream: PrintStream): Unit = {
    table match {
      case Seq() =>
      case Seq(header) =>
        val csvFormat = CSVFormat.RFC4180
          .withHeader(header: _*)
          .withDelimiter(',')
          .withRecordSeparator('\n')

        csvFormat.print(printStream)
          .close()
      case Seq(header, data @ _*) =>
        val csvFormat = CSVFormat.RFC4180
          .withHeader(header: _*)
          .withDelimiter(',')
          .withRecordSeparator('\n')

        for (printer <- managed(csvFormat.print(printStream));
             row <- data) {
          printer.printRecord(row: _*)
        }
    }
  }

  def outputSummary(deposits: Deposits, depositor: Option[DepositorId] = None)(implicit printStream: PrintStream): Unit = {
    val selectedDeposits = filterDepositsByDepositor(deposits, depositor)
    val depositsGroupedByState = groupAndSortDepositsAlphabeticallyByState(selectedDeposits)

    val now = Calendar.getInstance().getTime
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val currentTime = format.format(now)
    lazy val stateLength = depositsGroupedByState.map { case (state, _) => state.toString.length }.max

    printStream.println("Grand totals:")
    printStream.println("-------------")
    printStream.println(s"Timestamp          : $currentTime")
    printStream.println(f"Number of deposits : ${ selectedDeposits.size }%10d")
    printStream.println(s"Total space        : ${ formatStorageSize(selectedDeposits.map(_.storageSpace).sum) }")
    printStream.println()
    printStream.println("Per state:")
    printStream.println("----------")
    depositsGroupedByState.foreach { case (state, toBePrintedDeposits) => printLineForDepositGroup(state, toBePrintedDeposits, stateLength) }
    printStream.println()
  }

  def outputDeletedDeposits(deposits: Deposits)(implicit printStream: PrintStream): Unit = printDeleteRecord(deposits)

  private def printRecords(deposits: Deposits)(implicit printStream: PrintStream): Unit = {
    val csvFormat: CSVFormat = CSVFormat.RFC4180
      .withHeader("DEPOSITOR", "DEPOSIT_ID", "BAG_NAME", "DEPOSIT_STATE", "ORIGIN", "LOCATION", "DOI", "DOI_REGISTERED", "FEDORA_ID", "DATAMANAGER", "DEPOSIT_CREATION_TIMESTAMP",
        "DEPOSIT_UPDATE_TIMESTAMP", "DESCRIPTION", "NBR_OF_CONTINUED_DEPOSITS", "STORAGE_IN_BYTES")
      .withDelimiter(',')
      .withRecordSeparator('\n')

    for (printer <- managed(csvFormat.print(printStream));
         deposit <- deposits.sortBy(_.creationTimestamp)) {
      printer.printRecord(
        deposit.depositor,
        deposit.depositId,
        deposit.bagDirName,
        deposit.state,
        deposit.origin,
        deposit.location,
        deposit.doiIdentifier,
        deposit.registeredString,
        deposit.fedoraIdentifier,
        deposit.datamanager,
        deposit.creationTimestamp,
        deposit.lastModified,
        deposit.description,
        deposit.numberOfContinuedDeposits.toString,
        deposit.storageSpace.toString,
      )
    }
  }

  private def printDeleteRecord(deposits: Deposits)(implicit printStream: PrintStream): Unit = {
    val csvFormat: CSVFormat = CSVFormat.RFC4180
      .withHeader("DEPOSITOR", "DEPOSIT_ID", "BAG_NAME", "DEPOSIT_STATE", "ORIGIN", "LOCATION", "DOI", "DOI_REGISTERED", "FEDORA_ID", "DATAMANAGER", "DEPOSIT_CREATION_TIMESTAMP",
        "DEPOSIT_UPDATE_TIMESTAMP", "DESCRIPTION")
      .withDelimiter(',')
      .withRecordSeparator('\n')

    for (printer <- managed(csvFormat.print(printStream));
         deposit <- deposits.sortBy(_.creationTimestamp)) {
      printer.printRecord(
        deposit.depositor,
        deposit.depositId,
        deposit.bagDirName,
        deposit.state,
        deposit.origin,
        deposit.location,
        deposit.doiIdentifier,
        deposit.registeredString,
        deposit.fedoraIdentifier,
        deposit.datamanager,
        deposit.creationTimestamp,
        deposit.lastModified,
        deposit.description,
      )
    }
  }

  private def printLineForDepositGroup(state: State, depositGroup: Seq[DepositInformation], maxStateLength: Int)(implicit printStream: PrintStream): Unit = {
    printStream.println(formatCountAndSize(depositGroup, state, maxStateLength))
  }

  private def formatStorageSize(nBytes: Long): String = {
    def formatSize(unitSize: Long, unit: String): String = {
      f"${ nBytes / unitSize.toFloat }%8.1f $unit"
    }

    if (nBytes > 1.1 * TB) formatSize(TB, "T")
    else if (nBytes > 1.1 * GB) formatSize(GB, "G")
         else if (nBytes > 1.1 * MB) formatSize(MB, "M")
              else if (nBytes > 1.1 * KB) formatSize(KB, "K")
                   else formatSize(1, "B")
  }

  private def formatCountAndSize(deposits: Seq[DepositInformation], filterOnState: State, maxStateLength: Int): String = {
    s"%-${ maxStateLength }s : %5d (%s)".format(filterOnState, deposits.size, formatStorageSize(deposits.map(_.storageSpace).sum))
  }
}
