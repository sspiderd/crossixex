package crossix.ex

import java.io.{File, PrintWriter}
import java.nio.file.Files

import scala.collection.mutable.ListBuffer
import scala.io.Source

object CSVSorter {

	def getListOfFiles(dir: String): List[File] = {
		val d = new File(dir)
		if (d.exists && d.isDirectory) {
			d.listFiles.toList
		} else {
			List[File]()
		}
	}

	def indexOfHeaderToSortBy(headerLine: String, headerToSortBy: String): Int = {
		val headers = headerLine.split(",").map(_.trim)
		for ((x, i) <- headers.view.zipWithIndex) {
			if (headerToSortBy == x) {
				return i
			}
		}
		throw new Exception(s"""Could not find header $headerToSortBy in file""")
	}


	def unsortedFileToSortedFiles(inputCSV: Source, outputFolder: File, headerToSortBy: String,
								  availableMemory: Int, processNumber: Int = 0, parallelism: Int = 1): Unit = {
		var currentFileIndex = 0
		var currentLineIndex = 0
		var batchItems = ListBuffer.empty[List[String]]

		val lines = inputCSV.getLines()
		val csvHeaders = lines.next
		val sortFieldIndex = indexOfHeaderToSortBy(csvHeaders, headerToSortBy)
		for (line <- lines) {
			if (currentLineIndex % parallelism == processNumber) {
				batchItems.addOne(line.split(",").toList)
				// Since we are going to apply quicksort to merge the initial chuncks we must
				// Leave extra memory for the sorting process (timsort, the default scala sorter,
				// has space complexity of O(n) so we need to leave 2x memory initially).
				// This will not change the complexity as the
				// algorithm itself will require just one additional iteration.
				// However for simplicity's sake let's pretend that timsort doesn't use any memory
				// (otherwise i'll have to 'm/2' all the calculations)
				if (batchItems.length >= availableMemory) {
					// Initial sort and write to file
					batchItems = batchItems.sortBy(_ (sortFieldIndex))
					writeBatchToDisk(csvHeaders, batchItems.toList, outputFolder, currentFileIndex, processNumber)
					currentFileIndex += 1
					batchItems = ListBuffer.empty[List[String]]
				}
			}
			currentLineIndex += 1
		}
		// This happens when the reading ends.
		if (batchItems.nonEmpty) {
			batchItems = batchItems.sortBy(_ (sortFieldIndex))
			writeBatchToDisk(csvHeaders, batchItems.toList, outputFolder, currentFileIndex, processNumber)
		}
	}


	def writeBatchToDisk(csvHeaders: String, list: List[List[String]], outputFolder: File, fileIndex: Int, processNumber: Int = 0) = {
		val dir = new File(s"${outputFolder.getAbsolutePath}")
		if (!dir.exists()) {
			dir.mkdir()
		}
		val filename = s"$dir/${processNumber}_$fileIndex.csv"
		val pw = new PrintWriter(new File(filename))
		pw.println(csvHeaders)
		for (line <- list) {
			pw.println(line.mkString(","))
		}
		pw.close()
		println(s"""Process $processNumber wrote ${list.length} lines to $filename""")
	}

	def mergeSortedFiles(in1: Source, in2: Source, out: File, headerToSortBy: String, processNumber: Int = 0): Unit = {
		if (in2 == null) {
			val outWriter = new PrintWriter(out)
			for (line <- in1.getLines()) {
				outWriter.println(line)
			}
			outWriter.close()
			return
		}
		val outWriter = new PrintWriter(out)
		val lines1 = in1.getLines()
		val lines2 = in2.getLines()

		var linesWritten = 0

		val csvHeaders = lines1.next
		outWriter.println(csvHeaders)
		// Skip headers
		lines2.next
		val sortFieldIndex = indexOfHeaderToSortBy(csvHeaders, headerToSortBy)
		var line1 = lines1.next
		var line2 = lines2.next

		while (line1 != null && line2 != null) {
			val value1 = line1.split(",")(sortFieldIndex)
			val value2 = line2.split(",")(sortFieldIndex)

			if (value1 < value2) {
				outWriter.println(line1)
				linesWritten += 1
				line1 = lines1.hasNext match {
					case true => lines1.next()
					case false => {
						// Print the current line because once we exit the 'while' loop we will lose it
						outWriter.println(line2)
						linesWritten += 1
						null
					}
				}
			} else {
				outWriter.println(line2)
				linesWritten += 1
				line2 = lines2.hasNext match {
					case true => lines2.next()
					case false => {
						// Print the current line because once we exit the 'while' loop we will lose it
						outWriter.println(line1)
						linesWritten += 1
						null
					}
				}
			}
		}

		val extraLines = line1 match {
			case null => lines2
			case _ => lines1
		}

		for (line <- extraLines) {
			outWriter.println(line)
			linesWritten += 1
		}
		outWriter.close()
		println(s"""Process $processNumber wrote ${linesWritten} lines to ${out.getAbsolutePath}""")
	}

	def mergeSortedFilesInDirectory(inputFolder: File, outputFolder: File, headerToSortBy: String,
									processNumber: Int = 0, parallelism: Int = 1): Unit = {
		val files = getListOfFiles(inputFolder.getAbsolutePath)
		if (files.length == 1) {
			return
		}
		val filesForProcess = ListBuffer.empty[File]

		//At least one process MUST take 2 files, otherwise we can be in a situation
		//Where every process takes just one file and thus they'll never be merged
		//(Imagine 3 files left with 3 parallel processes)
		if (processNumber == 0) {
			filesForProcess.addOne(files(0))
		}

		for (i <- (processNumber + 1) until files.length by parallelism) {
			filesForProcess.addOne(files(i))
		}
		for (i <- 0 until filesForProcess.length by 2) {
			val outFile = new File(s"${outputFolder.getAbsolutePath}/${processNumber}_$i.csv")
			//Last file that has no partner
			if (i+1 == filesForProcess.length) {
				mergeSortedFiles(
					Source.fromFile(filesForProcess(i)), null,
					outFile, headerToSortBy, processNumber)
			} else {
				mergeSortedFiles(
					Source.fromFile(filesForProcess(i)), Source.fromFile(filesForProcess(i + 1)),
					outFile, headerToSortBy, processNumber)
			}
		}
	}

	/**
	 * This is the main method for the algorithm
	 */
	def sortLargeFile(input: File, output: File, headerToSortBy: String, availableMemory: Int, paralellism: Int = 1) = {
		if (availableMemory < 2) {
			throw new Exception("Must have at least '2' available memory")
		}
		var outDir = Files.createTempDirectory("csv_sort_").toFile

		var threads = ListBuffer.empty[CSVSorterThread]
		for (process <- 0 until paralellism) {
			threads.append(new CSVSorterThread(process, paralellism) {
				override def run(): Unit = {
					val source = Source.fromFile(input)
					CSVSorter.unsortedFileToSortedFiles(source, outDir, headerToSortBy, availableMemory, process, paralellism)
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
		var remainingFiles = getListOfFiles(outDir.getAbsolutePath).length
		println(s"First sorted result in ${outDir.getAbsolutePath} -> Number of files in directory: $remainingFiles")
		while (remainingFiles > 2) {
			val inDir = outDir
			outDir = Files.createTempDirectory("csv_sort_").toFile
			threads = ListBuffer.empty[CSVSorterThread]
			for (process <- 0 until paralellism) {
				threads.append(new CSVSorterThread(process, paralellism) {
					override def run(): Unit = {
						val source = Source.fromFile(input)
						CSVSorter.mergeSortedFilesInDirectory(inDir, outDir, headerToSortBy, process, paralellism)
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
			remainingFiles = getListOfFiles(outDir.getAbsolutePath).length
			println(s"Intermediate result written to ${outDir.getAbsolutePath} -> Number of files in directory: $remainingFiles")

		}
		val last2 = getListOfFiles(outDir.getAbsolutePath)
		if (last2.length == 1) {
			mergeSortedFiles(Source.fromFile(last2(0)), null, output, headerToSortBy)
		} else {
			mergeSortedFiles(Source.fromFile(last2(0)), Source.fromFile(last2(1)), output, headerToSortBy)
		}
		print(s"Final result in ${output.getAbsolutePath}")
	}

	def main(args: Array[String]): Unit = {
		val input = new File(args(0))
		val output = new File(args(1))
		val headerToSortBy = args(2)
		val memory = args(3).toInt
		val parallelism = args(4).toInt
		sortLargeFile(input, output, headerToSortBy, memory, parallelism)
	}

}