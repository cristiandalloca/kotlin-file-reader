package br.cdb.filereader.parser.impl

import br.cdb.filereader.parser.FileParser
import org.apache.poi.openxml4j.opc.OPCPackage
import org.apache.poi.ss.usermodel.DataFormatter
import org.apache.poi.util.XMLHelper
import org.apache.poi.xssf.eventusermodel.XSSFReader
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler
import org.springframework.stereotype.Component
import org.xml.sax.InputSource
import java.io.InputStream
import java.util.concurrent.LinkedBlockingQueue
import java.util.stream.Stream
import kotlin.concurrent.thread

@Component
class ExcelFileParser : FileParser {

    override fun supports(fileExtension: String): Boolean {
        return "xlsx".equals(fileExtension, ignoreCase = true)
                || "xls".equals(fileExtension, ignoreCase = true)
    }

    override fun parse(inputStream: InputStream): Stream<String> {
        val queue = LinkedBlockingQueue<String>()

        val poisonPill = "POISON_PILL_END_OF_STREAM"

        val parsingThread = thread(start = true, name = "xlsx-sax-parser") {
            try {
                OPCPackage.open(inputStream).use { pkg ->
                    val reader = XSSFReader(pkg)
                    val styles = reader.stylesTable
                    val strings = reader.sharedStringsTable
                    val formatter = DataFormatter()

                    val handler = SheetToQueueHandler(queue)
                    val sheetParser = XMLHelper.newXMLReader()
                    sheetParser.contentHandler = XSSFSheetXMLHandler(styles, strings, handler, false)

                    val sheets = reader.sheetsData
                    while (sheets.hasNext()) {
                        sheets.next().use { sheetStream ->
                            sheetParser.parse(InputSource(sheetStream))
                        }
                    }
                }
            } catch (e: Exception) {
                println("Error during SAX parsing: ${e.message}")
            } finally {
                queue.put(poisonPill)
            }
        }

        return Stream.generate { queue.take() }
            .takeWhile { it != poisonPill }
            .onClose {
                parsingThread.interrupt()
            }
    }

    /**
     * Um handler customizado que recebe eventos do parser SAX.
     * Quando uma linha é concluída, ela a formata e a adiciona à fila.
     */
    private class SheetToQueueHandler(
        private val queue: LinkedBlockingQueue<String>
    ) : XSSFSheetXMLHandler.SheetContentsHandler {

        private val currentRow = mutableListOf<String>()

        override fun startRow(rowNum: Int) {
            currentRow.clear()
        }

        override fun endRow(rowNum: Int) {
            if (currentRow.isNotEmpty()) {
                queue.put(currentRow.joinToString(";"))
            }
        }

        override fun cell(cellReference: String?, formattedValue: String?, comment: org.apache.poi.xssf.usermodel.XSSFComment?) {
            val value = formattedValue ?: ""
            currentRow.add(value)
        }
    }

}