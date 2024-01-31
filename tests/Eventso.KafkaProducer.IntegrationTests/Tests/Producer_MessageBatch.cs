#pragma warning disable xUnit1026

using Confluent.Kafka;
using FluentAssertions;
using Xunit;

namespace Eventso.KafkaProducer.IntegrationTests.Tests
{
    [Collection("Global collection")]
    public class Producer_MessageBatch : IDisposable
    {
        private readonly string bootstrapServers;
        private readonly TemporaryTopic topic;
        private readonly IProducer producer;
        
        public Producer_MessageBatch(GlobalFixture globalFixture)
        {
            bootstrapServers = globalFixture.bootstrapServers;
            topic = new TemporaryTopic(globalFixture.bootstrapServers, 1);
            producer = new ProducerBuilder(new ProducerConfig { BootstrapServers = globalFixture.bootstrapServers }).Build();
        }

        public void Dispose()
        {
            producer.Dispose();
            topic.Dispose();
        }

        [Fact]
        public async Task Producer_MessageBatch_Bytes()
        {
            var messages = Enumerable.Range(0, 100)
                .Select(CreateMessage)
                .ToArray();
            
            var batch = producer.CreateBatch(topic.Name);

            foreach (var message in messages)
                batch.Produce(message.key, message.value);

            await batch.Complete();
            
            using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
                { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() }).Build();

            consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));

            foreach (var message in messages)
            {
                var recordedMessage = consumer.Consume(TimeSpan.FromSeconds(10));

                recordedMessage.Message.Key.Should().BeEquivalentTo(message.key);
                recordedMessage.Message.Value.Should().BeEquivalentTo(message.value);
            }
        }

        [Fact]
        public async Task Producer_MessageBatch_FastDelivery()
        {
            var messages = Enumerable.Range(0, 100)
                .Select(CreateMessage)
                .ToArray();

            var batch = producer.CreateBatch(topic.Name);
            var count = 0;

            foreach (var message in messages)
            {
                count++;
                batch.Produce(message.key, message.value);
                if (count % 5 == 0)
                    await Task.Delay(10);
            }

            await batch.Complete();

            using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
                { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() }).Build();

            consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));

            foreach (var message in messages)
            {
                var recordedMessage = consumer.Consume(TimeSpan.FromSeconds(10));

                recordedMessage.Message.Key.Should().BeEquivalentTo(message.key);
                recordedMessage.Message.Value.Should().BeEquivalentTo(message.value);
            }
        }

        [Fact]
        public async Task Producer_MessageBatch_Bytes_MultiPartition()
        {
            var messages = Enumerable.Range(0, 100)
                .Select(CreateMessage)
                .ToArray();

            using var topic = new TemporaryTopic(bootstrapServers, 3);
            var headers = new Headers
            {
                { "test", new byte[] { 12, 17, 56 } },
                { "test-2", new byte[] { 14, 188, 36 } }
            };

            var batch = producer.CreateBatch(topic.Name);
            int index = 0;
            var messagesPerPartition = new Dictionary<int, List<(byte[] key, byte[] value)>>(3)
            {
                { 0, new() },
                { 1, new() },
                { 2, new() }
            };

            foreach (var message in messages)
            {
                var partition = index % 3;
                batch.Produce(message.key, message.value, headers, partition: partition);

                messagesPerPartition[partition].Add(message);

                index++;
            }

            await batch.Complete();

            using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
                { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() }).Build();

            foreach (var messagesPart in messagesPerPartition)
            {
                consumer.Assign(new TopicPartitionOffset(topic.Name, messagesPart.Key, 0));

                foreach (var message in messagesPart.Value)
                {
                    var recordedMessage = consumer.Consume(TimeSpan.FromSeconds(10));

                    recordedMessage.Message.Key.Should().BeEquivalentTo(message.key);
                    recordedMessage.Message.Value.Should().BeEquivalentTo(message.value);
                    foreach (var header in headers)
                    {
                        recordedMessage.Message.Headers.GetLastBytes(header.Key).Should().BeEquivalentTo(header.GetValueBytes());
                    }
                }

                consumer.Unassign();
            }
        }

        [Fact]
        public async Task Producer_MessageBatch_CompleteEmpty()
        {
            var batch = producer.CreateBatch("any");

            var act = () => batch.Complete();
            await act.Should().NotThrowAsync();
        }

        [Fact]
        public async Task Producer_MessageBatch_ProduceToCompleted_Throws()
        {
            var messages = Enumerable.Range(0, 3)
                .Select(CreateMessage)
                .ToArray();

            var batch = producer.CreateBatch(topic.Name);

            foreach (var message in messages)
                batch.Produce(message.key, message.value);
            await batch.Complete();

            var act = () => batch.Produce(messages[0].key, messages[0].value);
            act.Should().Throw<InvalidOperationException>();
        }

        [Fact]
        public async Task Producer_MessageBatch_CompleteBeforeDelivery_Waiting()
        {
            var messages = Enumerable.Range(0, 10)
                .Select(CreateMessage)
                .ToArray();

            var batch = producer.CreateBatch(topic.Name);

            foreach (var message in messages)
                batch.Produce(message.key, message.value);

            var task = batch.Complete();

            task.IsCompleted.Should().BeFalse();
            await task;

            using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
                { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() }).Build();

            consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));

            foreach (var message in messages)
            {
                var recordedMessage = consumer.Consume(TimeSpan.FromSeconds(10));

                recordedMessage.Message.Key.Should().BeEquivalentTo(message.key);
                recordedMessage.Message.Value.Should().BeEquivalentTo(message.value);
            }
        }


        private static (byte[] key, byte[] value) CreateMessage(int n)
        {
            return (
                new byte[] { (byte)n, (byte)(n + 1), (byte)(n + 2) },
                new byte[] { (byte)(n + 5), (byte)(n + 10), (byte)(n + 11), (byte)(n + 15) });
        }

        [Fact]
        public async Task Producer_MessageBatch_ShortKey()
        {
            const short key1 = 25678;
            const short key2 = -25678;
            const short key3 = short.MaxValue;
            const short key4 = short.MinValue;
            var value = new byte[] { 33 };

            var batch = producer.CreateBatch(topic.Name);

            batch.Produce(key1, value);
            batch.Produce(key2, value, partition: 0);
            batch.Produce(key3, value);
            batch.Produce(key4, value, partition: 0);

            await batch.Complete();

            var consumerConfig = new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() };

            using var consumer = new ConsumerBuilder<short, byte[]>(consumerConfig)
                .SetKeyDeserializer(new ShortDeserializer())
                .Build();

            consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));
            var r1 = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.Equal(key1, r1.Message.Key);

            var r2 = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.Equal(key2, r2.Message.Key);

            var r3 = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.Equal(key3, r3.Message.Key);

            var r4 = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.Equal(key4, r4.Message.Key);
        }

        [Fact]
        public async Task Producer_MessageBatch_IntKey()
        {
            const int key1 = 256780;
            const int key2 = -256780;
            const int key3 = int.MaxValue;
            const int key4 = int.MinValue;
            var value = new byte[] { 33 };

            var batch = producer.CreateBatch(topic.Name);

            batch.Produce(key1, value);
            batch.Produce(key2, value, partition: 0);
            batch.Produce(key3, value);
            batch.Produce(key4, value, partition: 0);

            await batch.Complete();

            var consumerConfig = new ConsumerConfig
                { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() };

            using var consumer = new ConsumerBuilder<int, byte[]>(consumerConfig).Build();

            consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));
            var r1 = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.Equal(key1, r1.Message.Key);

            var r2 = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.Equal(key2, r2.Message.Key);

            var r3 = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.Equal(key3, r3.Message.Key);

            var r4 = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.Equal(key4, r4.Message.Key);
        }

        [Fact]
        public async Task Producer_MessageBatch_LongKey()
        {
            const long key1 = 25678000000;
            const long key2 = -25678000000;
            const long key3 = long.MaxValue;
            const long key4 = long.MinValue;
            var value = new byte[] { 33 };

            var batch = producer.CreateBatch(topic.Name);

            batch.Produce(key1, value);
            batch.Produce(key2, value, partition: 0);
            batch.Produce(key3, value);
            batch.Produce(key4, value, partition: 0);

            await batch.Complete();

            var consumerConfig = new ConsumerConfig
                { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() };

            using var consumer = new ConsumerBuilder<long, byte[]>(consumerConfig).Build();

            consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));
            var r1 = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.Equal(key1, r1.Message.Key);

            var r2 = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.Equal(key2, r2.Message.Key);

            var r3 = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.Equal(key3, r3.Message.Key);

            var r4 = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.Equal(key4, r4.Message.Key);
        }

        [Fact]
        public async Task Producer_MessageBatch_StringKey()
        {
            string key1 = Guid.NewGuid().ToString();
            string key2 = "QWERTYUIO#";
            string key3 = string.Empty;
            string key4 = new string('x', 8000);
            string? key5 = null;
            var value = new byte[] { 33 };

            var batch = producer.CreateBatch(topic.Name);

            batch.Produce(key1, value);
            batch.Produce(key2, value, partition: 0);
            batch.Produce(key3, value);
            batch.Produce(key4, value, partition: 0);
            batch.Produce(key5, value);

            await batch.Complete();

            var consumerConfig = new ConsumerConfig
                { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() };

            using var consumer = new ConsumerBuilder<string, byte[]>(consumerConfig).Build();

            consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));
            var r1 = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.Equal(key1, r1.Message.Key);

            var r2 = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.Equal(key2, r2.Message.Key);

            var r3 = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.Equal(key3, r3.Message.Key);

            var r4 = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.Equal(key4, r4.Message.Key);

            var r5 = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.Equal(key5, r5.Message.Key);
        }

        [Fact]
        public async Task Producer_MessageBatch_GuidKey()
        {
            var keys = new[]
            {
                Guid.NewGuid(),
                Guid.Empty,
                Guid.NewGuid(),
                Guid.NewGuid()
            };
            var value = new byte[] { 33 };

            var batch = producer.CreateBatch(topic.Name);

            for (var index = 0; index < keys.Length; index++)
            {
                if (index % 2 == 0)
                    batch.Produce(keys[index], value, partition: 0);
                else
                    batch.Produce(keys[index], value);
            }


            await batch.Complete();

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = Guid.NewGuid().ToString()
            };

            using var consumer = new ConsumerBuilder<Guid, byte[]>(consumerConfig)
                .SetKeyDeserializer(new GuidDeserializer())
                .Build();

            consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));

            foreach (var key in keys)
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(10));
                result.Message.Key.Should().Be(key);
            }
        }
    }
}