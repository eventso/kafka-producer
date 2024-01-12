// Copyright 2018 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

#pragma warning disable xUnit1026

using Confluent.Kafka;
using FluentAssertions;
using Xunit;

namespace Eventso.KafkaProducer.IntegrationTests.Tests
{
    /// <summary>
    ///     Test a variety of cases where a producer is constructed
    ///     using the handle from another producer.
    /// </summary>
    public partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public async Task Producer_MessageBatch_Bytes(string bootstrapServers)
        {
            var messages = Enumerable.Range(0, 100)
                .Select(CreateMessage)
                .ToArray();

            using var topic = new TemporaryTopic(bootstrapServers, 1);
            using var producer = new ProducerBuilder(new ProducerConfig { BootstrapServers = bootstrapServers }).Build();

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

        [Theory, MemberData(nameof(KafkaParameters))]
        public async Task Producer_MessageBatch_Bytes_MultyPartition(string bootstrapServers)
        {
            var messages = Enumerable.Range(0, 100)
                .Select(CreateMessage)
                .ToArray();

            using var topic = new TemporaryTopic(bootstrapServers, 3);
            using var producer = new ProducerBuilder(new ProducerConfig { BootstrapServers = bootstrapServers }).Build();
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
            }
        }

        [Theory, MemberData(nameof(KafkaParameters))]
        public async Task Producer_MessageBatch_CompleteEmpty(string bootstrapServers)
        {
            using var producer = new ProducerBuilder(new ProducerConfig { BootstrapServers = bootstrapServers }).Build();

            var batch = producer.CreateBatch("any");

            var act = () => batch.Complete();
            await act.Should().NotThrowAsync();
        }

        [Theory, MemberData(nameof(KafkaParameters))]
        public async Task Producer_MessageBatch_ProduceToCompleted_Throws(string bootstrapServers)
        {
            var messages = Enumerable.Range(0, 3)
                .Select(CreateMessage)
                .ToArray();

            using var topic = new TemporaryTopic(bootstrapServers, 1);
            using var producer = new ProducerBuilder(new ProducerConfig { BootstrapServers = bootstrapServers }).Build();

            var batch = producer.CreateBatch(topic.Name);

            foreach (var message in messages)
                batch.Produce(message.key, message.value);
            await batch.Complete();

            var act = () => batch.Produce(messages[0].key, messages[0].value);
            act.Should().Throw<InvalidOperationException>();
        }

        [Theory, MemberData(nameof(KafkaParameters))]
        public async Task Producer_MessageBatch_CompleteBeforeDelivery_Waiting(string bootstrapServers)
        {
            var messages = Enumerable.Range(0, 10)
                .Select(CreateMessage)
                .ToArray();

            using var topic = new TemporaryTopic(bootstrapServers, 1);
            using var producer = new ProducerBuilder(new ProducerConfig { BootstrapServers = bootstrapServers, LingerMs = 5_000 }).Build();

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

        [Theory, MemberData(nameof(KafkaParameters))]
        public async Task Producer_MessageBatch_ShortKey(string bootstrapServers)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            const short key1 = 25678;
            const short key2 = -25678;
            const short key3 = short.MaxValue;
            const short key4 = short.MinValue;
            var value = new byte[] { 33 };

            using var topic = new TemporaryTopic(bootstrapServers, 1);
            using var producer = new ProducerBuilder(producerConfig).Build();


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

        [Theory, MemberData(nameof(KafkaParameters))]
        public async Task Producer_MessageBatch_IntKey(string bootstrapServers)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            const int key1 = 256780;
            const int key2 = -256780;
            const int key3 = int.MaxValue;
            const int key4 = int.MinValue;
            var value = new byte[] { 33 };

            using var topic = new TemporaryTopic(bootstrapServers, 1);
            using var producer = new ProducerBuilder(producerConfig).Build();
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

        [Theory, MemberData(nameof(KafkaParameters))]
        public async Task Producer_MessageBatch_LongKey(string bootstrapServers)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            const long key1 = 25678000000;
            const long key2 = -25678000000;
            const long key3 = long.MaxValue;
            const long key4 = long.MinValue;
            var value = new byte[] { 33 };

            using var topic = new TemporaryTopic(bootstrapServers, 1);
            using var producer = new ProducerBuilder(producerConfig).Build();
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

        [Theory, MemberData(nameof(KafkaParameters))]
        public async Task Producer_MessageBatch_StringKey(string bootstrapServers)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            string key1 = Guid.NewGuid().ToString();
            string key2 = "QWERTYUIO#";
            string key3 = string.Empty;
            string key4 = new string('x', 8000);
            string? key5 = null;
            var value = new byte[] { 33 };

            using var topic = new TemporaryTopic(bootstrapServers, 1);
            using var producer = new ProducerBuilder(producerConfig).Build();
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

        [Theory, MemberData(nameof(KafkaParameters))]
        public async Task Producer_MessageBatch_GuidKey(string bootstrapServers)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            var key1 = Guid.NewGuid();
            var key2 = Guid.Empty;
            var key3 = Guid.NewGuid();
            var key4 = Guid.NewGuid();
            var value = new byte[] { 33 };

            using var topic = new TemporaryTopic(bootstrapServers, 1);
            using var producer = new ProducerBuilder(producerConfig).Build();

            var batch = producer.CreateBatch(topic.Name);

            batch.Produce(key1, value);
            batch.Produce(key2, value, partition: 0);
            batch.Produce(key3, value);
            batch.Produce(key4, value, partition: 0);

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
            var r1 = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.Equal(key1, r1.Message.Key);

            var r2 = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.Equal(key2, r2.Message.Key);

            var r3 = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.Equal(key3, r3.Message.Key);

            var r4 = consumer.Consume(TimeSpan.FromSeconds(10));
            Assert.Equal(key4, r4.Message.Key);
        }
    }
}