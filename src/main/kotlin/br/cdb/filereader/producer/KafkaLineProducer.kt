package br.cdb.filereader.producer

import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.util.concurrent.CompletableFuture

@Service
class KafkaLineProducer(
    private val kafkaTemplate: KafkaTemplate<String, String>
) {

    @Value("\${kafka.topic.name}")
    private lateinit var topicName: String

    fun sendMessageAsync(message: String): CompletableFuture<*> {
        return kafkaTemplate.send(topicName, message)
    }
}