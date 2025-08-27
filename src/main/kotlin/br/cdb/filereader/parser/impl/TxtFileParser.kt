package br.cdb.filereader.parser.impl

import br.cdb.filereader.parser.FileParser
import org.springframework.stereotype.Component
import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.util.stream.Stream

@Component
class TxtFileParser : FileParser {

    override fun supports(fileExtension: String): Boolean = "txt".equals(fileExtension, ignoreCase = true)

    override fun parse(inputStream: InputStream): Stream<String> {
        return inputStream.bufferedReader(StandardCharsets.UTF_8).lines()
    }
}