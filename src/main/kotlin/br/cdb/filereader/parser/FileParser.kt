package br.cdb.filereader.parser

import java.io.InputStream
import java.util.stream.Stream

interface FileParser {

    fun supports(fileExtension: String): Boolean

    fun parse(inputStream: InputStream): Stream<String>

}