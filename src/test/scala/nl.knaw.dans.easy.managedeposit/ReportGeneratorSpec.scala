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

import nl.knaw.dans.easy.managedeposit.ReportType.ReportType
import nl.knaw.dans.easy.managedeposit.State._
import org.joda.time.DateTime
import org.scalamock.scalatest.MockFactory
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers

import java.io.{ ByteArrayOutputStream, PrintStream }
import java.text.SimpleDateFormat
import java.util.{ Calendar, UUID }

class ReportGeneratorSpec extends TestSupportFixture
  with Matchers
  with MockFactory
  with Inspectors {

  private implicit val dansDoiPrefixes: List[String] = List("10.17026/", "10.5072/")

  "filterDepositsByDepositor" should "only return deposits where the id of the depositor matches" in {
    val deposits = List(
      createDeposit("dans-1", DRAFT, "SRC2"),
      createDeposit("dans-1", SUBMITTED, "SRC1"),
      createDeposit("dans-3", SUBMITTED, "SRC1"),
    )
    ReportGenerator.filterDepositsByDepositor(deposits, Some("dans-1")).size shouldBe 2
  }

  it should "should return empty list if none of the id's matches" in {
    val deposits = List(
      createDeposit("dans-2", DRAFT, "SRC2"),
      createDeposit("dans-5", SUBMITTED, "SRC1"),
      createDeposit("dans-3", SUBMITTED, "SRC1"),
    )
    ReportGenerator.filterDepositsByDepositor(deposits, Some("dans-1")) shouldBe empty
  }

  it should "should return all deposits if the given depositorId is empty" in {
    val deposits = List(
      createDeposit("dans-2", DRAFT, "SRC2"),
      createDeposit("dans-5", SUBMITTED, "SRC1"),
      createDeposit("dans-3", SUBMITTED, "SRC1"),
    )
    ReportGenerator.filterDepositsByDepositor(deposits, None).size shouldBe 3
  }

  it should "should return all deposits if the given depositorId is empty and one of the depositorIds is null" in {
    val deposits = List(
      createDeposit(null, DRAFT, "SRC2"),
      createDeposit("dans-5", SUBMITTED, "SRC1"),
      createDeposit("dans-3", SUBMITTED, "SRC1"),
    )
    ReportGenerator.filterDepositsByDepositor(deposits, None).size shouldBe 3
  }

  it should "should skip all depositorId = null deposits if the given depositorId is given" in {
    val deposits = List(
      createDeposit(null, DRAFT, "SRC2"),
      createDeposit("dans-1", SUBMITTED, "SRC1"),
      createDeposit("dans-1", SUBMITTED, "SRC1"),
    )
    ReportGenerator.filterDepositsByDepositor(deposits, Some("dans-1")).size shouldBe 2
  }

  "groupDepositsByState" should "return a map with all deposits sorted By their state and null should be mapped to UNKNOWN" in {
    val deposits: List[DepositInformation] = createDeposits
    val mappedByState = ReportGenerator.groupAndSortDepositsAlphabeticallyByState(deposits).toMap
    mappedByState.getOrElse(ARCHIVED, Seq()).size shouldBe 2
    mappedByState.getOrElse(DRAFT, Seq()).size shouldBe 1
    mappedByState.getOrElse(FINALIZING, Seq()).size shouldBe 1
    mappedByState.getOrElse(INVALID, Seq()).size shouldBe 1
    mappedByState.getOrElse(REJECTED, Seq()).size shouldBe 1
    mappedByState.getOrElse(SUBMITTED, Seq()).size shouldBe 4
    mappedByState.getOrElse(UNKNOWN, Seq()).size shouldBe 4 // 2 + 2 null values
  }

  "output Summary" should "should contain all deposits" in {
    val deposits = createDeposits
    val now = Calendar.getInstance().getTime
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val currentTime = format.format(now)
    val baos = new ByteArrayOutputStream()
    val ps = new PrintStream(baos, true)
    try {
      ReportGenerator.outputSummary(deposits, Some("dans-1"))(ps)
    } finally {
      ps.close()
    }

    baos.toString.trim shouldBe
      s"""Grand totals:
         |-------------
         |Timestamp          : $currentTime
         |Number of deposits :         16
         |Total space        :      2.0 M
         |
         |Per state:
         |----------
         |ARCHIVED        :     2 (   252.0 K)
         |DRAFT           :     1 (   126.0 K)
         |FEDORA_ARCHIVED :     1 (   126.0 K)
         |FINALIZING      :     1 (   126.0 K)
         |IN_REVIEW       :     1 (   126.0 K)
         |INVALID         :     1 (   126.0 K)
         |REJECTED        :     1 (   126.0 K)
         |SUBMITTED       :     4 (   503.9 K)
         |UNKNOWN         :     4 (   503.9 K)""".stripMargin
  }

  it should "produce a report when no deposits are found" in {
    val now = Calendar.getInstance().getTime
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val currentTime = format.format(now)
    val baos = new ByteArrayOutputStream()
    val ps = new PrintStream(baos, true)
    try {
      ReportGenerator.outputSummary(Nil, Some("dans-1"))(ps)
    } finally {
      ps.close()
    }

    baos.toString.trim shouldBe
      s"""Grand totals:
         |-------------
         |Timestamp          : $currentTime
         |Number of deposits :          0
         |Total space        :      0.0 B
         |
         |Per state:
         |----------""".stripMargin
  }

  it should "align the data per state based on the longest state" in {
    val deposits = createDeposits.filterNot(_.state == FEDORA_ARCHIVED)
    val now = Calendar.getInstance().getTime
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val currentTime = format.format(now)
    val baos = new ByteArrayOutputStream()
    val ps = new PrintStream(baos, true)
    try {
      ReportGenerator.outputSummary(deposits, Some("dans-1"))(ps)
    } finally {
      ps.close()
    }

    baos.toString.trim shouldBe
      s"""Grand totals:
         |-------------
         |Timestamp          : $currentTime
         |Number of deposits :         15
         |Total space        :      1.8 M
         |
         |Per state:
         |----------
         |ARCHIVED   :     2 (   252.0 K)
         |DRAFT      :     1 (   126.0 K)
         |FINALIZING :     1 (   126.0 K)
         |IN_REVIEW  :     1 (   126.0 K)
         |INVALID    :     1 (   126.0 K)
         |REJECTED   :     1 (   126.0 K)
         |SUBMITTED  :     4 (   503.9 K)
         |UNKNOWN    :     4 (   503.9 K)""".stripMargin
  }

  "outputErrorReport" should "only print the deposits containing an error" in {
    val baos = new ByteArrayOutputStream()
    val errorDeposit = createDeposit("dans-0", ARCHIVED, "SRC1").copy(dansDoiRegistered = Some(false)) //violates the rule ARCHIVED must be registered when DANS doi
    val noDansDoiDeposit = createDeposit("dans-1", ARCHIVED, "SRC1").copy(dansDoiRegistered = Some(false), doiIdentifier = "11.11111/other-doi-123")
    val ps: PrintStream = new PrintStream(baos, true)
    val deposits = List(
      errorDeposit,
      noDansDoiDeposit, //does not violate any rule
      createDeposit("dans-2", SUBMITTED, "SRC1"), //does not violate any rule
      createDeposit("dans-3", SUBMITTED, "SRC1"), //does not violate any rule
    )
    outputReportManged(ps, deposits, ReportType.ERROR)
    val errorReport = baos.toString
    errorReport should include(createCsvRow(errorDeposit)) // only the first deposit should be added to the report
    errorReport should not include createCsvRow(noDansDoiDeposit)
  }

  it should "not print any csv rows if no deposits violate the rules" in {
    val baos = new ByteArrayOutputStream()
    val ps: PrintStream = new PrintStream(baos, true)
    val deposits = List(
      createDeposit("dans-0", DRAFT, "SRC2").copy(dansDoiRegistered = Some(false)),
      createDeposit("dans-1", SUBMITTED, "SRC1"),
      createDeposit("dans-1", SUBMITTED, "SRC1"),
    )
    outputReportManged(ps, deposits, ReportType.ERROR)

    val errorReport = baos.toString
    deposits.foreach(deposit => errorReport should not include createCsvRow(deposit)) // None of the deposits should be added to the report
  }

  it should "print any deposit that has one of the states null, UNKNOWN, INVALID, FAILED, REJECTED or ARCHIVED + not-registered" in {
    val baos = new ByteArrayOutputStream()
    val ps: PrintStream = new PrintStream(baos, true)
    val deposits = List(
      createDeposit("dans-0", ARCHIVED, "SRC1").copy(dansDoiRegistered = Some(false)), //violates the rule ARCHIVED must be registered
      createDeposit("dans-1", FAILED, "SRC1"),
      createDeposit("dans-2", REJECTED, "SRC1"),
      createDeposit("dans-3", INVALID, "SRC2"),
      createDeposit("dans-4", UNKNOWN, "SRC1"),
      createDeposit("dans-5", null, "SRC1"),
    )
    outputReportManged(ps, deposits, ReportType.ERROR)

    val errorReport = baos.toString
    forEvery(deposits)(deposit => errorReport should include(createCsvRow(deposit))) //all deposits should be added to the report
  }

  it should "leave out deposits originating from easy-deposit-api that are rejected by a datamanager after review" in {
    val baos = new ByteArrayOutputStream()
    val ps: PrintStream = new PrintStream(baos, true)
    val depositsInReport = List(
      createDeposit("dans-0", ARCHIVED, "SRC1").copy(dansDoiRegistered = Some(false)), //violates the rule ARCHIVED must be registered
      createDeposit("dans-1", FAILED, "SRC1"),
      createDeposit("dans-2", REJECTED, "SRC1"),
      createDeposit("dans-3", INVALID, "SRC2"),
      createDeposit("dans-4", UNKNOWN, "SRC1"),
      createDeposit("dans-5", null, "SRC1"),
    )
    val depositsNotInReport = List(
      createDeposit("dans-rejected", REJECTED, "SRC1").copy(origin = "API", description = Curation.requestChangesDescription),
      createDeposit("dans-abandoned-draft", INVALID, "SRC1").copy(origin = "SWORD2", description = "abandoned draft, data removed"),
    )
    outputReportManged(ps, depositsInReport ::: depositsNotInReport, ReportType.ERROR)

    val errorReport = baos.toString
    errorReport.linesIterator.toList should have length depositsInReport.size + 1 // 1x header + |depositsInReport|
    forEvery(depositsInReport)(deposit => errorReport should include(createCsvRow(deposit)))
    forEvery(depositsNotInReport)(deposit => errorReport should not include createCsvRow(deposit))
  }

  "outputFullReport" should "print all deposits" in {
    val baos = new ByteArrayOutputStream()
    val ps: PrintStream = new PrintStream(baos, true)
    val deposits = createDeposits
    outputReportManged(ps, deposits, ReportType.FULL)
    val fullReport = baos.toString
    forEvery(deposits)(deposit => fullReport should include(createCsvRow(deposit)))
  }

  "outputRawReport" should "not print anything on an empty table" in {
    val baos = new ByteArrayOutputStream()
    val ps: PrintStream = new PrintStream(baos, true)
    val table: Seq[Seq[String]] = Seq.empty
    ReportGenerator.outputRawReport(table)(ps)
    val report = baos.toString()
    report shouldBe empty
  }

  it should "only print the headers when only one row is provided" in {
    val baos = new ByteArrayOutputStream()
    val ps: PrintStream = new PrintStream(baos, true)
    val table: Seq[Seq[String]] = Seq(
      Seq("a", "b", "c", "d"),
    )
    ReportGenerator.outputRawReport(table)(ps)
    val report = baos.toString()
    report shouldBe
      """a,b,c,d
        |""".stripMargin
  }

  it should "only print the report when both headers and data are provided" in {
    val baos = new ByteArrayOutputStream()
    val ps: PrintStream = new PrintStream(baos, true)
    val table: Seq[Seq[String]] = Seq(
      Seq("a", "b", "c", "d"),
      Seq("1", "2", "3", "n/a"),
      Seq("4", "n/a", "5", "n/a"),
      Seq("n/a", "6", "7", "8"),
    )
    ReportGenerator.outputRawReport(table)(ps)
    val report = baos.toString()
    report shouldBe
      """a,b,c,d
        |1,2,3,n/a
        |4,n/a,5,n/a
        |n/a,6,7,8
        |""".stripMargin
  }

  private def outputReportManged(ps: PrintStream, deposits: List[DepositInformation], reportType: ReportType): Unit = {
    try {
      reportType match {
        case ReportType.ERROR => ReportGenerator.outputErrorReport(deposits)(ps)
        case ReportType.FULL => ReportGenerator.outputFullReport(deposits)(ps)
      }
    } finally {
      ps.close()
    }
  }

  private def createCsvRow(deposit: DepositInformation): String = {
    s"${ deposit.depositor }," +
      s"${ deposit.depositId }," +
      s"${ deposit.bagDirName }," +
      s"${ Option(deposit.state).getOrElse("") }," +
      s"${ deposit.origin }," +
      s"${ deposit.location }," +
      s"${ deposit.doiIdentifier }," +
      s"${ deposit.registeredString }," +
      s"${ deposit.fedoraIdentifier }," +
      s"${ deposit.datamanager }," +
      s"${ deposit.creationTimestamp }," +
      s"${ deposit.lastModified }," +
      s"${ deposit.description }," +
      s"${ deposit.numberOfContinuedDeposits }," +
      s"${ deposit.storageSpace }"
  }

  private def createDeposit(depositorId: String, state: State, location: String): DepositInformation = {
    DepositInformation(UUID.randomUUID().toString, "10.17026/dans-12345", Some(true), "FedoraId", depositorId, state, "", DateTime.now().minusDays(3).toString(), 2, 129000, "", "SWORD2", location, "baggy", "my-datamanager")
  }

  private def createDeposits = List(
    createDeposit("dans-1", ARCHIVED, "SRC1"),
    createDeposit("dans-1", ARCHIVED, "SRC1"),
    createDeposit("dans-1", DRAFT, "SRC2"),
    createDeposit("dans-1", FINALIZING, "SRC2"),
    createDeposit("dans-1", INVALID, "SRC1"),
    createDeposit("dans-1", REJECTED, "SRC1"),
    createDeposit("dans-1", IN_REVIEW, "SRC1"),
    createDeposit("dans-1", FEDORA_ARCHIVED, "SRC1"),
    createDeposit("dans-1", SUBMITTED, "SRC1"),
    createDeposit("dans-1", SUBMITTED, "SRC1"),
    createDeposit("dans-1", SUBMITTED, "SRC1"),
    createDeposit("dans-1", SUBMITTED, "SRC1"), // duplicate deposits are allowed
    createDeposit("dans-1", UNKNOWN, "SRC2"),
    createDeposit("dans-1", UNKNOWN, "SRC1"),
    createDeposit("dans-1", null, "SRC1"), // mapped and added to unknown
    createDeposit("dans-1", null, "SRC1"),
  )
}

object ReportType extends Enumeration {
  type ReportType = Value
  val FULL: ReportType = Value("FULL")
  val ERROR: ReportType = Value("EROOR")
}

