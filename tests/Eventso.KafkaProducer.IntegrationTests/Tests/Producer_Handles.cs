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
        [Theory, MemberData(nameof(KafkaProducersParameters))]
        public async Task Producer_Handles(string bootstrapServers, TestProducerType producerType)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            using (var topic = new TemporaryTopic(bootstrapServers, 1))
            {
                using (var producer1 = new TestProducerBuilder<byte[], byte[]>(producerConfig, producerType).Build())
                using (var producer2 = new TestDependentProducerBuilder<string, string>(producer1.Handle, producerType).Build())
                using (var producer3 = new TestDependentProducerBuilder<byte[], byte[]>(producer1.Handle, producerType).Build())
                using (var producer4 = new TestDependentProducerBuilder<int, string>(producer2.Handle, producerType).Build())
                using (var producer5 = new TestDependentProducerBuilder<int, int>(producer3.Handle, producerType).Build())
                using (var producer6 = new TestDependentProducerBuilder<string, byte[]>(producer4.Handle, producerType).Build())
                using (var producer7 = new TestProducerBuilder<double, double>(producerConfig, producerType).Build())
                using (var adminClient = new DependentAdminClientBuilder(producer7.Handle).Build())
                {
                    var r1 = await producer1.ProduceAsync(topic.Name, new Message<byte[], byte[]> { Key = new byte[] { 42 }, Value = new byte[] { 33 } });
                    Assert.Equal(new byte[] { 42 }, r1.Key);
                    Assert.Equal(new byte[] { 33 }, r1.Value);
                    Assert.Equal(0, r1.Offset);

                    var r2 = await producer2.ProduceAsync(topic.Name, new Message<string, string> { Key = "hello", Value = "world" });
                    Assert.Equal("hello", r2.Key);
                    Assert.Equal("world", r2.Value);

                    var r3 = await producer3.ProduceAsync(topic.Name, new Message<byte[], byte[]> { Key = new byte[] { 40 }, Value = new byte[] { 31 } });
                    Assert.Equal(new byte[] { 40 }, r3.Key);
                    Assert.Equal(new byte[] { 31 }, r3.Value);

                    var r4 = await producer4.ProduceAsync(topic.Name, new Message<int, string> { Key = 42, Value = "mellow world" });
                    Assert.Equal(42, r4.Key);
                    Assert.Equal("mellow world", r4.Value);

                    var r5 = await producer5.ProduceAsync(topic.Name, new Message<int, int> { Key = int.MaxValue, Value = int.MinValue });
                    Assert.Equal(int.MaxValue, r5.Key);
                    Assert.Equal(int.MinValue, r5.Value);

                    var r6 = await producer6.ProduceAsync(topic.Name, new Message<string, byte[]> { Key = "yellow mould", Value = new byte[] { 69 } });
                    Assert.Equal("yellow mould", r6.Key);
                    Assert.Equal(new byte[] { 69 }, r6.Value);

                    var r7 = await producer7.ProduceAsync(topic.Name, new Message<double, double> { Key = 44.0, Value = 234.4 });
                    Assert.Equal(44.0, r7.Key);
                    Assert.Equal(234.4, r7.Value);

                    var topicMetadata = adminClient.GetMetadata(singlePartitionTopic, TimeSpan.FromSeconds(10));
                    Assert.Single(topicMetadata.Topics);

                    // implicitly check this does not throw.
                }

                var consumerConfig = new ConsumerConfig { BootstrapServers = bootstrapServers, GroupId = Guid.NewGuid().ToString() };

                using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
                {
                    consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 0));
                    var r1 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(new byte[] { 42 }, r1.Message.Key);
                    Assert.Equal(new byte[] { 33 }, r1.Message.Value);
                    Assert.Equal(0, r1.Offset);
                }

                using (var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build())
                {
                    consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 1));
                    var r2 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal("hello", r2.Message.Key);
                    Assert.Equal("world", r2.Message.Value);
                    Assert.Equal(1, r2.Offset);
                }

                using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
                {
                    consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 2));
                    var r3 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(new byte[] { 40 }, r3.Message.Key);
                    Assert.Equal(new byte[] { 31 }, r3.Message.Value);
                    Assert.Equal(2, r3.Offset);
                }

                using (var consumer = new ConsumerBuilder<int, string>(consumerConfig).Build())
                {
                    consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 3));
                    var r4 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(42, r4.Message.Key);
                    Assert.Equal("mellow world", r4.Message.Value);
                    Assert.Equal(3, r4.Offset);
                }

                using (var consumer = new ConsumerBuilder<int, int>(consumerConfig).Build())
                {
                    consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 4));
                    var r5 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(int.MaxValue, r5.Message.Key);
                    Assert.Equal(int.MinValue, r5.Message.Value);
                    Assert.Equal(4, r5.Offset);
                }

                using (var consumer = new ConsumerBuilder<string, byte[]>(consumerConfig).Build())
                {
                    consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 5));
                    var r6 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal("yellow mould", r6.Message.Key);
                    Assert.Equal(new byte[] { 69 }, r6.Message.Value);
                    Assert.Equal(5, r6.Offset);
                }

                using (var consumer = new ConsumerBuilder<double, double>(consumerConfig).Build())
                {
                    consumer.Assign(new TopicPartitionOffset(topic.Name, 0, 6));
                    var r7 = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.Equal(44.0, r7.Message.Key);
                    Assert.Equal(234.4, r7.Message.Value);
                    Assert.Equal(6, r7.Offset);
                }
            }

            Assert.Equal(0, Library.HandleCount);
        }
    }
}