package br.cdb.filereader.parser

import br.cdb.filereader.exception.UnsupportedFileTypeException
import org.springframework.stereotype.Component

@Component
class FileParserFactory(
    private val parsers: List<FileParser>
) {

    fun getParser(fileName: String): FileParser {
        val fileExtension = fileName.substringAfterLast(".", "")
        return parsers.firstOrNull { it.supports(fileExtension) }
            ?: throw UnsupportedFileTypeException("Tipo de arquivo não suportado: $fileExtension")
    }
}