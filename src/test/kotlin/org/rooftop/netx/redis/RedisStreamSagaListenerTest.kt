package org.rooftop.netx.redis

import com.fasterxml.jackson.databind.ObjectMapper
import io.kotest.assertions.nondeterministic.eventually
import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import org.rooftop.netx.engine.AbstractSagaDispatcher
import org.rooftop.netx.engine.core.Saga
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.connection.stream.MapRecord
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.stream.StreamReceiver
import reactor.core.publisher.Flux
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.seconds

internal class RedisStreamSagaListenerTest : DescribeSpec({

    describe("subscribeStream 메소드는") {
        context("기존 Redis StreamReceiver가 에러로 종료되면") {
            it("새 StreamReceiver를 생성해서 다시 구독한다") {
                val sagaDispatcher = mockk<AbstractSagaDispatcher>(relaxed = true)
                val connectionFactory = mockk<ReactiveRedisConnectionFactory>(relaxed = true)
                val reactiveRedisTemplate = mockk<ReactiveRedisTemplate<String, Saga>>(relaxed = true)
                val objectMapper = ObjectMapper()

                val receiverCreateCount = AtomicInteger()
                val receiverFactory = {
                    val receiver = mockk<StreamReceiver<String, MapRecord<String, String, String>>>()
                    val createCount = receiverCreateCount.incrementAndGet()

                    every {
                        receiver.receive(any(), any())
                    } returns when (createCount) {
                        1 -> Flux.error(IllegalStateException("disconnected"))
                        else -> Flux.never()
                    }

                    receiver
                }

                val listener = RedisStreamSagaListener(
                    backpressureSize = 40,
                    sagaDispatcher = sagaDispatcher,
                    connectionFactory = connectionFactory,
                    nodeGroup = "api",
                    nodeName = "api-1",
                    reactiveRedisTemplate = reactiveRedisTemplate,
                    objectMapper = objectMapper,
                    receiverFactory = receiverFactory,
                    groupInitializer = { Flux.just("OK") },
                )

                listener.subscribeStream()

                eventually(3.seconds) {
                    receiverCreateCount.get() shouldBe 2
                }
            }
        }
    }
})
