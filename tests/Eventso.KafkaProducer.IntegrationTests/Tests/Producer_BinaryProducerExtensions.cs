#pragma warning disable xUnit1026

using Confluent.Kafka;
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
        public async Task Producer_Binary_ShortKey(string bootstrapServers)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            const short key1 = 25678;
            const short key2 = -25678;
            const short key3 = short.MaxValue;
            const short key4 = short.MinValue;

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            {
                using (var producer1 = new ProducerBuilder(producerConfig).Build())
                {
                    var value = new byte[] { 33 };

                    var r1 = await producer1.ProduceAsync(topic.Name, key1, value);
                    Assert.Equal(0, r1.Offset);

                    await producer1.ProduceAsync(topic.Name, key2, value, partition: 0);
                    producer1.Produce(topic.Name, key3, value);
                    producer1.Produce(topic.Name, key4, value, partition: 0);
                }

                var consumerConfig = new ConsumerConfig
                    { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() };

                using (var consumer = new ConsumerBuilder<short, byte[]>(consumerConfig)
                    .SetKeyDeserializer(new ShortDeserializer())
                    .Build())
                {
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

            Assert.Equal(0, Library.HandleCount);
        }

        [Theory, MemberData(nameof(KafkaParameters))]
        public async Task Producer_Binary_IntKey(string bootstrapServers)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            const int key1 = 256780;
            const int key2 = -256780;
            const int key3 = int.MaxValue;
            const int key4 = int.MinValue;

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            {
                using (var producer1 = new ProducerBuilder(producerConfig).Build())
                {
                    var value = new byte[] { 33 };

                    var r1 = await producer1.ProduceAsync(topic.Name, key1, value);
                    Assert.Equal(0, r1.Offset);

                    await producer1.ProduceAsync(topic.Name, key2, value, partition: 0);
                    producer1.Produce(topic.Name, key3, value);
                    producer1.Produce(topic.Name, key4, value, partition: 0);
                }

                var consumerConfig = new ConsumerConfig
                    { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() };

                using (var consumer = new ConsumerBuilder<int, byte[]>(consumerConfig).Build())
                {
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

            Assert.Equal(0, Library.HandleCount);
        }

        [Theory, MemberData(nameof(KafkaParameters))]
        public async Task Producer_Binary_LongKey(string bootstrapServers)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            const long key1 = 25678000000;
            const long key2 = -25678000000;
            const long key3 = long.MaxValue;
            const long key4 = long.MinValue;

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            {
                using (var producer1 = new ProducerBuilder(producerConfig).Build())
                {
                    var value = new byte[] { 33 };

                    var r1 = await producer1.ProduceAsync(topic.Name, key1, value);
                    Assert.Equal(0, r1.Offset);

                    await producer1.ProduceAsync(topic.Name, key2, value, partition: 0);
                    producer1.Produce(topic.Name, key3, value);
                    producer1.Produce(topic.Name, key4, value, partition: 0);
                }

                var consumerConfig = new ConsumerConfig
                    { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() };

                using (var consumer = new ConsumerBuilder<long, byte[]>(consumerConfig).Build())
                {
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

            Assert.Equal(0, Library.HandleCount);
        }

        [Theory, MemberData(nameof(KafkaParameters))]
        public async Task Producer_Binary_StringKey(string bootstrapServers)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            string key1 = Guid.NewGuid().ToString();
            string key2 = "QWERTYUIO#";
            string key3 = string.Empty;
            string key4 = new string('x', 8000);
            string? key5 = null;

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            {
                using (var producer1 = new ProducerBuilder(producerConfig).Build())
                {
                    var value = new byte[] { 33 };

                    var r1 = await producer1.ProduceAsync(topic.Name, key1, value);
                    Assert.Equal(0, r1.Offset);

                    await producer1.ProduceAsync(topic.Name, key2, value, partition: 0);
                    producer1.Produce(topic.Name, key3, value);
                    producer1.Produce(topic.Name, key4, value, partition: 0);

                    await producer1.ProduceAsync(topic.Name, key5, value, partition: 0);
                    producer1.Produce(topic.Name, key5, value);
                }

                var consumerConfig = new ConsumerConfig
                    { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() };

                using (var consumer = new ConsumerBuilder<string, byte[]>(consumerConfig).Build())
                {
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

                    var r6 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(key5, r6.Message.Key);
                }
            }

            Assert.Equal(0, Library.HandleCount);
        }

        [Theory, MemberData(nameof(KafkaParameters))]
        public async Task Producer_Binary_GuidKey(string bootstrapServers)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            var key1 = Guid.NewGuid();
            var key2 = Guid.Empty;
            var key3 = Guid.NewGuid();
            var key4 = Guid.NewGuid();

            using var topic = new TemporaryTopic(bootstrapServers, 1);
            using (var producer1 = new ProducerBuilder(producerConfig).Build())
            {
                var value = new byte[] { 33 };

                var dr1 = await producer1.ProduceAsync(topic.Name, key1, value);
                Assert.Equal(0, dr1.Offset);

                await producer1.ProduceAsync(topic.Name, key2, value, partition: 0);
                producer1.Produce(topic.Name, key3, value);
                producer1.Produce(topic.Name, key4, value, partition: 0);
            }

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = Guid.NewGuid().ToString()
            };

            using (var consumer = new ConsumerBuilder<Guid, byte[]>(consumerConfig)
                .SetKeyDeserializer(new GuidDeserializer())
                .Build())
            {
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


            Assert.Equal(0, Library.HandleCount);
        }
    }
}