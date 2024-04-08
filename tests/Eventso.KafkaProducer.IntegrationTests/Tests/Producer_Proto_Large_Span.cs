#pragma warning disable xUnit1026

using System.Buffers.Binary;
using AutoFixture;
using Confluent.Kafka;
using FluentAssertions;
using Tests;
using Xunit;

namespace Eventso.KafkaProducer.IntegrationTests.Tests
{
    [Collection("Global collection")]
    public class Producer_Proto_Large_Span : IDisposable
    {
        private readonly string bootstrapServers;
        private readonly TemporaryTopic topic;
        private readonly IProducer producer;
        private readonly OrderProto testProto;

        public Producer_Proto_Large_Span(GlobalFixture globalFixture)
        {
            bootstrapServers = globalFixture.bootstrapServers;
            topic = new TemporaryTopic(globalFixture.bootstrapServers, 1);
            producer = new ProducerBuilder(new ProducerConfig { BootstrapServers = globalFixture.bootstrapServers }).Build();
            var fixture = new Fixture();
            fixture.RepeatCount = 100;
            fixture.Inject(new string('x',1024));

            testProto = fixture.Create<OrderProto>();
        }

        public void Dispose()
        {
            producer.Dispose();
            topic.Dispose();
        }

        [Fact]
        public async Task ShortKey()
        {
            short[] keys =
            {
                25678,
                -25678,
                short.MaxValue,
                short.MinValue
            };

            var destination = new byte[2];

            foreach (var key in keys)
                await producer.ProduceAsync(topic.Name, key, testProto);

            foreach (var key in keys)
            {
                BinaryPrimitives.WriteInt16BigEndian(destination, key);
                await producer.ProduceAsync(topic.Name, destination, testProto);
            }

            foreach (var key in keys)
                producer.Produce(topic.Name, key, testProto);

            foreach (var key in keys)
            {
                BinaryPrimitives.WriteInt16BigEndian(destination, key);
                producer.Produce(topic.Name, destination, testProto);
            }

            var consumerConfig = new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() };

            using var consumer = new ConsumerBuilder<short, OrderProto>(consumerConfig)
                .SetKeyDeserializer(new ShortDeserializer())
                .SetValueDeserializer(new OrderProtoDeserializer())
                .Build();

            consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));

            foreach (var key in Enumerable.Repeat(keys, 4).SelectMany(x => x))
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(10));
                result.Message.Key.Should().Be(key);
                result.Message.Value.Should().BeEquivalentTo(testProto);
            }
        }

        [Fact]
        public async Task IntKey()
        {
            int[] keys =
            {
                256780,
                -256780,
                int.MaxValue,
                int.MinValue
            };

            foreach (var key in keys)
                await producer.ProduceAsync(topic.Name, key, testProto);

            foreach (var key in keys)
                producer.Produce(topic.Name, key, testProto);


            var consumerConfig = new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() };

            using var consumer = new ConsumerBuilder<int, OrderProto>(consumerConfig)
                .SetValueDeserializer(new OrderProtoDeserializer())
                .Build();

            consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));

            foreach (var key in Enumerable.Repeat(keys, 2).SelectMany(x => x))
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(10));
                result.Message.Key.Should().Be(key);
                result.Message.Value.Should().BeEquivalentTo(testProto);
            }
        }

        [Fact]
        public async Task LongKey()
        {
            long[] keys =
            {
                25678000000,
                -25678000000,
                long.MaxValue,
                long.MinValue
            };

            foreach (var key in keys)
                await producer.ProduceAsync(topic.Name, key, testProto);

            foreach (var key in keys)
                producer.Produce(topic.Name, key, testProto);


            var consumerConfig = new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() };

            using var consumer = new ConsumerBuilder<long, OrderProto>(consumerConfig)
                .SetValueDeserializer(new OrderProtoDeserializer())
                .Build();

            consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));

            foreach (var key in Enumerable.Repeat(keys, 2).SelectMany(x => x))
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(10));
                result.Message.Key.Should().Be(key);
                result.Message.Value.Should().BeEquivalentTo(testProto);
            }
        }

        [Fact]
        public async Task StringKey()
        {
            string?[] keys =
            {
                Guid.NewGuid().ToString(),
                "QWERTYUIO#",
                string.Empty,
                new string('x', 8000),
                null
            };

            foreach (var key in keys)
                await producer.ProduceAsync(topic.Name, key, testProto);

            foreach (var key in keys)
                producer.Produce(topic.Name, key, testProto);


            var consumerConfig = new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() };

            using var consumer = new ConsumerBuilder<string, OrderProto>(consumerConfig)
                .SetValueDeserializer(new OrderProtoDeserializer())
                .Build();

            consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));

            foreach (var key in Enumerable.Repeat(keys, 2).SelectMany(x => x))
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(10));
                result.Message.Key.Should().Be(key);
                result.Message.Value.Should().BeEquivalentTo(testProto);
            }
        }

        [Fact]
        public async Task GuidKey()
        {
            var keys = new[]
            {
                Guid.NewGuid(),
                Guid.Empty,
                Guid.NewGuid(),
                Guid.NewGuid()
            };

            foreach (var key in keys)
                await producer.ProduceAsync(topic.Name, key, testProto);

            foreach (var key in keys)
                producer.Produce(topic.Name, key, testProto);


            var consumerConfig = new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() };

            using var consumer = new ConsumerBuilder<Guid, OrderProto>(consumerConfig)
                .SetValueDeserializer(new OrderProtoDeserializer())
                .SetKeyDeserializer(new GuidDeserializer())
                .Build();

            consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));

            foreach (var key in Enumerable.Repeat(keys, 2).SelectMany(x => x))
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(10));
                result.Message.Key.Should().Be(key);
                result.Message.Value.Should().BeEquivalentTo(testProto);
            }
        }
    }
}