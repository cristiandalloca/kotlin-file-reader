package br.cdb.filereader.runner

import br.cdb.filereader.parser.FileParserFactory
import br.cdb.filereader.producer.KafkaLineProducer
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
import org.springframework.stereotype.Component
import java.io.FileInputStream
import java.util.concurrent.atomic.AtomicLong

@Component
class FileProcessingRunner(
    private val fileParserFactory: FileParserFactory,
    private val kafkaLineProducer: KafkaLineProducer
) : CommandLineRunner {

    companion object {
        private val log = LoggerFactory.getLogger(FileProcessingRunner::class.java)
    }

    override fun run(vararg args: String) {
        if (args.isEmpty()) {
            log.error("É necessário fornecer o caminho do arquivo como argumento.")
            return
        }

        val filePath = args[0]
        log.info("Iniciando processamento do arquivo: {}", filePath)

        val successCounter = AtomicLong(0)
        val failureCounter = AtomicLong(0)

        try {
            val parser = fileParserFactory.getParser(filePath)
            log.info("Utilizando o parser: {}", parser.javaClass.simpleName)

            FileInputStream(filePath).use { inputStream ->
                parser.parse(inputStream).use { lines ->
                    lines.parallel().forEach { line ->
                        kafkaLineProducer.sendMessageAsync(line)
                            .whenComplete { _, exception ->
                                if (exception == null) {
                                    successCounter.incrementAndGet()
                                } else {
                                    log.error("Falha ao enviar mensagem para o Kafka: {}", line, exception)
                                    failureCounter.incrementAndGet()
                                }
                            }
                    }
                }
            }
            log.info("Processamento do arquivo concluído. Sucessos: {}, Falhas: {}", successCounter.get(), failureCounter.get())
        } catch (e: Exception) {
            log.error("Ocorreu um erro fatal ao processar o arquivo: {}", e.message, e)
        }

    }
}