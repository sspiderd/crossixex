package crossix.ex

import java.io.{File, PrintWriter}
import java.nio.file.Files

import org.assertj.core.api.Assertions
import org.scalatest.FlatSpec

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random

class ExTests extends FlatSpec {

	val random = new Random()

	def getListOfFiles(dir: String): List[File] = {
		val d = new File(dir)
		if (d.exists && d.isDirectory) {
			d.listFiles.toList
		} else {
			List[File]()
		}
	}

	"Initial Sorting with enough memory and a single process" should "create exactly one file" in {
		val source = Source.fromURL(getClass.getResource("/test1.csv"))
		val tempDir = Files.createTempDirectory(s"csv_tests_${random.nextInt()}").toFile
		CSVSorter.unsortedFileToSortedFiles(source, tempDir, "header1", Integer.MAX_VALUE)
		source.close()

		val files = getListOfFiles(s"${tempDir.getAbsolutePath}")
		Assertions.assertThat(files.length).isEqualTo(1)
		Assertions.assertThat(files(0).getName).isEqualTo("0_0.csv")
	}

	"Initial Sorting with enough memory and a single process" should "store the file in a sorted manner and include header" in {
		val source = Source.fromURL(getClass.getResource("/test1.csv"))
		val tempDir = Files.createTempDirectory(s"csv_tests_${random.nextInt()}").toFile
		CSVSorter.unsortedFileToSortedFiles(source, tempDir, "header1", Integer.MAX_VALUE)
		source.close()

		val res = Source.fromFile(s"${tempDir.getAbsolutePath}/0_0.csv")
		val lines = res.getLines().toList
		Assertions.assertThat(lines(0).startsWith("header1"))
		for (i <- 2 until lines.length) {
			Assertions.assertThat(lines(i - 1)(0) < lines(i)(0))
		}
	}

	for (availableMemory <- 2 to 6) {
		for (numberOfProcesses <- 1 to 4) {
			s"Initial sorting for large files with (availableMemory=$availableMemory and numberOfProcesses=$numberOfProcesses" should "create 'n/m' sorted files with headers, each containing 'm' entries" in {

				val numberOfOriginalLines = Source.fromURL(getClass.getResource("/test1.csv")).getLines().length - 1

				val tempDir = Files.createTempDirectory(s"csv_tests_${random.nextInt()}").toFile

				val threads = new ListBuffer[Thread]

				for (process <- 0 until numberOfProcesses) {
					threads.append(new CSVSorterThread(process, numberOfProcesses) {
						override def run(): Unit = {
							val source = Source.fromURL(getClass.getResource("/test1.csv"))
							CSVSorter.unsortedFileToSortedFiles(source, tempDir, "header1", availableMemory, process, numberOfProcesses)
							source.close()
						}
					})
				}

				for (thread <- threads) {
					thread.start()
				}

				for (thread <- threads) {
					thread.join()
				}

				val resultingFiles = getListOfFiles(s"${tempDir.getAbsolutePath}")
				for (file <- resultingFiles) {
					val lines = Source.fromFile(file).getLines().toList
					Assertions.assertThat(lines(0).startsWith("header1"))
					Assertions.assertThat(lines.length).isGreaterThan(0)
					Assertions.assertThat(lines.length - 1).isLessThanOrEqualTo(availableMemory)
					for (i <- 2 until lines.length) {
						Assertions.assertThat(lines(i - 1)(0) < lines(i)(0))
					}
				}
			}
		}
	}

	def writeToTempFile(content: String): File = {
		import java.io.File
		val file = File.createTempFile("pre-", ".csv")
		val writer = new PrintWriter(file)
		writer.write(content)
		writer.close()
		file
	}

	"Merging 2 sorted files" should "create a sorted file in the location, with headers" in {
		val s1 =
			"""	|sorton, dontsorton
				|aaa, aaa
				|ccc, ddd
				|""".stripMargin

		val s2 =
			"""	|sorton, dontsorton
				|bbb, fff
				|ddd, kkk
				|""".stripMargin

		val f1 = Source.fromFile(writeToTempFile(s1))
		val f2 = Source.fromFile(writeToTempFile(s2))

		val out = File.createTempFile("pre-", ".csv")

		CSVSorter.mergeSortedFiles(f1, f2, out, "sorton")

		val res = Source.fromFile(out).getLines().toList
		Assertions.assertThat(res.length).isEqualTo(5)
		Assertions.assertThat(res(0)).isEqualTo("sorton, dontsorton")
		Assertions.assertThat(res(1)).isEqualTo("aaa, aaa")
		Assertions.assertThat(res(2)).isEqualTo("bbb, fff")
		Assertions.assertThat(res(3)).isEqualTo("ccc, ddd")
		Assertions.assertThat(res(4)).isEqualTo("ddd, kkk")
	}

	for (availableMemory <- 2 to 6) {
		for (numberOfProcesses <- 1 to 4) {
			s"Sorting a large file with memory=$availableMemory and processes=$numberOfProcesses" should "result in a single sorted file in the output directory" in {
				val unsortedCSV = Source.fromURL(getClass.getResource("/test1.csv"))
				val linesInOriginalFile = unsortedCSV.getLines().toList
				val input = writeToTempFile(linesInOriginalFile.mkString("\n"))
				val output = File.createTempFile("result-", ".csv")
				CSVSorter.sortLargeFile(input, output, "header1", availableMemory, numberOfProcesses)

				val linesInOutputFile = Source.fromFile(output).getLines().toList


				// Drop the headers
				val sortedInput = linesInOriginalFile.drop(1).sorted
				val sortedOutput = linesInOutputFile.drop(1)
				Assertions.assertThat(sortedInput).isEqualToComparingFieldByField(sortedOutput)
			}
		}
	}

}
