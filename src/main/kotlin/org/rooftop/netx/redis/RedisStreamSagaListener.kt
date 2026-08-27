package org.rooftop.netx.redis

import com.fasterxml.jackson.databind.ObjectMapper
import io.lettuce.core.RedisBusyException
import org.rooftop.netx.engine.AbstractSagaDispatcher
import org.rooftop.netx.engine.AbstractSagaListener
import org.rooftop.netx.engine.core.Saga
import org.rooftop.netx.engine.logging.info
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.connection.stream.Consumer
import org.springframework.data.redis.connection.stream.MapRecord
import org.springframework.data.redis.connection.stream.ReadOffset
import org.springframework.data.redis.connection.stream.StreamOffset
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.stream.StreamReceiver
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import kotlin.time.Duration.Companion.hours
import kotlin.time.toJavaDuration

internal class RedisStreamSagaListener(
    backpressureSize: Int,
    sagaDispatcher: AbstractSagaDispatcher,
    private val connectionFactory: ReactiveRedisConnectionFactory,
    private val nodeGroup: String,
    private val nodeName: String,
    private val reactiveRedisTemplate: ReactiveRedisTemplate<String, Saga>,
    private val objectMapper: ObjectMapper,
    receiverFactory: (() -> StreamReceiver<String, MapRecord<String, String, String>>)? = null,
    groupInitializer: (() -> Flux<String>)? = null,
) : AbstractSagaListener(backpressureSize, sagaDispatcher) {

    private val options = StreamReceiver.StreamReceiverOptions.builder()
        .pollTimeout(1.hours.toJavaDuration())
        .build()

    private val receiverFactory = receiverFactory ?: {
        StreamReceiver.create(connectionFactory, options)
    }

    private val groupInitializer = groupInitializer ?: {
        createGroupIfNotExists()
    }

    override fun receive(): Flux<Pair<Saga, String>> {
        return groupInitializer()
            .flatMap {
                receiverFactory().receive(
                    Consumer.from(nodeGroup, nodeName),
                    StreamOffset.create(STREAM_KEY, ReadOffset.from(">"))
                ).map {
                    objectMapper.readValue(
                        it.value["data"],
                        Saga::class.java,
                    ) to it.id.value
                }
            }
    }

    private fun createGroupIfNotExists(): Flux<String> {
        return reactiveRedisTemplate.opsForStream<String, ByteArray>()
            .createGroup(STREAM_KEY, ReadOffset.from("0"), nodeGroup)
            .info("Redis stream group created key \"$STREAM_KEY\" group \"$nodeGroup\"")
            .onErrorResume {
                if (it.cause is RedisBusyException) {
                    return@onErrorResume Mono.just("OK")
                }
                throw it
            }
            .flatMapMany { Flux.just(it) }
    }

    override fun shutdownCascade() {
        connectionFactory.reactiveConnection.close()
    }

    private companion object {
        private const val STREAM_KEY = "NETX_STREAM"
    }
}
