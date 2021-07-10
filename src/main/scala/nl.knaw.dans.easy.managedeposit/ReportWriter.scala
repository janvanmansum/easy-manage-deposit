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

import org.apache.commons.csv.CSVFormat
import org.apache.commons.lang.StringUtils

import java.io.PrintStream

class ReportWriter(printStream: PrintStream) extends Function[DepositInformation, Unit] with AutoCloseable {
  private val csvFormat: CSVFormat = CSVFormat.RFC4180
    .withHeader("DEPOSITOR", "DEPOSIT_ID", "BAG_NAME", "DEPOSIT_STATE", "ORIGIN", "LOCATION", "DOI", "DOI_REGISTERED", "FEDORA_ID", "DATAMANAGER", "DEPOSIT_CREATION_TIMESTAMP",
      "DEPOSIT_UPDATE_TIMESTAMP", "DESCRIPTION", "NBR_OF_CONTINUED_DEPOSITS", "STORAGE_IN_BYTES")
    .withDelimiter(',')
    .withRecordSeparator('\n')
  private val printer = csvFormat.print(printStream)

  override def apply(di: DepositInformation): Unit = {
    printer.printRecord(
      di.depositor,
      di.depositId,
      di.bagDirName,
      di.state,
      di.origin,
      di.location,
      di.doiIdentifier,
      di.registeredString,
      di.fedoraIdentifier,
      di.datamanager,
      di.creationTimestamp,
      di.lastModified,
      StringUtils.abbreviate(di.description, 1000),
      di.numberOfContinuedDeposits.toString,
      di.storageSpace.toString)
  }

  override def close(): Unit = {
    printer.close()
  }
}
