package br.cdb.filereader

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class FileReaderApplication

fun main(args: Array<String>) {
    runApplication<FileReaderApplication>(*args)
}
