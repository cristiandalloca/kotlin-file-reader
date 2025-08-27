package br.cdb.filereader.parser.impl

import br.cdb.filereader.parser.FileParser
import com.opencsv.CSVReader
import org.springframework.stereotype.Component
import java.io.InputStream
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.stream.Stream
import java.util.stream.StreamSupport

@Component
class CsvFileParser : FileParser {

    override fun supports(fileExtension: String): Boolean = "csv".equals(fileExtension, ignoreCase = true)

    override fun parse(inputStream: InputStream): Stream<String> {
        val reader = CSVReader(InputStreamReader(inputStream, StandardCharsets.UTF_8))

        return StreamSupport.stream(reader.spliterator(), false)
            .map { line -> line.joinToString(";") }
            .onClose { reader.close() }
    }
}