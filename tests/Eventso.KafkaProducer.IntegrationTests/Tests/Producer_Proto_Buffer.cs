#pragma warning disable xUnit1026

using System.Buffers.Binary;
using AutoFixture;
using CommunityToolkit.HighPerformance.Buffers;
using Confluent.Kafka;
using FluentAssertions;
using Tests;
using Xunit;

namespace Eventso.KafkaProducer.IntegrationTests.Tests
{
    [Collection("Global collection")]
    public class Producer_Proto_Buffer : IDisposable
    {
        private readonly string bootstrapServers;
        private readonly TemporaryTopic topic;
        private readonly IProducer producer;
        private readonly OrderProto testProto;
        private readonly ArrayPoolBufferWriter<byte> buffer = new();

        public Producer_Proto_Buffer(GlobalFixture globalFixture)
        {
            bootstrapServers = globalFixture.bootstrapServers;
            topic = new TemporaryTopic(globalFixture.bootstrapServers, 1);
            producer = new ProducerBuilder(new ProducerConfig { BootstrapServers = globalFixture.bootstrapServers }).Build();
            var fixture = new Fixture();

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
            
            var batch = producer.CreateBatch(topic.Name);

            foreach (var key in keys)
                batch.Produce(key, testProto);

            await batch.Complete();

            var batch2 = producer.CreateBatch(topic.Name);

            foreach (var key in keys)
            {
                BinaryPrimitives.WriteInt16BigEndian(destination, key);
                batch2.Produce(destination, testProto);
            }

            await batch2.Complete();

            foreach (var key in keys)
                await producer.ProduceAsync(topic.Name, key, testProto, buffer);

            foreach (var key in keys)
            {
                BinaryPrimitives.WriteInt16BigEndian(destination, key);
                await producer.ProduceAsync(topic.Name, destination, testProto, buffer);
            }

            foreach (var key in keys)
                producer.Produce(topic.Name, key, testProto, buffer);

            foreach (var key in keys)
            {
                BinaryPrimitives.WriteInt16BigEndian(destination, key);
                producer.Produce(topic.Name, destination, testProto, buffer);
            }

            var consumerConfig = new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() };

            using var consumer = new ConsumerBuilder<short, OrderProto>(consumerConfig)
                .SetKeyDeserializer(new ShortDeserializer())
                .SetValueDeserializer(new OrderProtoDeserializer())
                .Build();

            consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));

            foreach (var key in Enumerable.Repeat(keys, 6).SelectMany(x => x))
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

            var batch = producer.CreateBatch(topic.Name);

            foreach (var key in keys)
                batch.Produce(key, testProto);

            await batch.Complete();

            foreach (var key in keys)
                await producer.ProduceAsync(topic.Name, key, testProto, buffer);

            foreach (var key in keys)
                producer.Produce(topic.Name, key, testProto, buffer);


            var consumerConfig = new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() };

            using var consumer = new ConsumerBuilder<int, OrderProto>(consumerConfig)
                .SetValueDeserializer(new OrderProtoDeserializer())
                .Build();

            consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));

            foreach (var key in Enumerable.Repeat(keys, 3).SelectMany(x => x))
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

            var batch = producer.CreateBatch(topic.Name);

            foreach (var key in keys)
                batch.Produce(key, testProto);

            await batch.Complete();

            foreach (var key in keys)
                await producer.ProduceAsync(topic.Name, key, testProto, buffer);

            foreach (var key in keys)
                producer.Produce(topic.Name, key, testProto, buffer);


            var consumerConfig = new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() };

            using var consumer = new ConsumerBuilder<long, OrderProto>(consumerConfig)
                .SetValueDeserializer(new OrderProtoDeserializer())
                .Build();

            consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));

            foreach (var key in Enumerable.Repeat(keys, 3).SelectMany(x => x))
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

            var batch = producer.CreateBatch(topic.Name);

            foreach (var key in keys)
                batch.Produce(key, testProto);

            await batch.Complete();

            foreach (var key in keys)
                await producer.ProduceAsync(topic.Name, key, testProto, buffer);

            foreach (var key in keys)
                producer.Produce(topic.Name, key, testProto, buffer);


            var consumerConfig = new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() };

            using var consumer = new ConsumerBuilder<string, OrderProto>(consumerConfig)
                .SetValueDeserializer(new OrderProtoDeserializer())
                .Build();

            consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));

            foreach (var key in Enumerable.Repeat(keys, 3).SelectMany(x => x))
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
            var batch = producer.CreateBatch(topic.Name);

            foreach (var key in keys)
                batch.Produce(key, testProto);

            await batch.Complete();

            foreach (var key in keys)
                await producer.ProduceAsync(topic.Name, key, testProto, buffer);

            foreach (var key in keys)
                producer.Produce(topic.Name, key, testProto, buffer);


            var consumerConfig = new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() };

            using var consumer = new ConsumerBuilder<Guid, OrderProto>(consumerConfig)
                .SetValueDeserializer(new OrderProtoDeserializer())
                .SetKeyDeserializer(new GuidDeserializer())
                .Build();

            consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));

            foreach (var key in Enumerable.Repeat(keys, 3).SelectMany(x => x))
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(10));
                result.Message.Key.Should().Be(key);
                result.Message.Value.Should().BeEquivalentTo(testProto);
            }
        }
    }
}