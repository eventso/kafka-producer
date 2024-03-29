// Copyright 2016-2017 Confluent Inc.
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

using System.Reflection;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Eventso.KafkaProducer.IntegrationTests.Tests
{
    public class GlobalFixture : IDisposable
    {
        public string bootstrapServers;

        public const int partitionedTopicNumPartitions = 2;

        public GlobalFixture()
        {
            var assemblyPath = typeof(Tests).GetTypeInfo().Assembly.Location;
            var assemblyDirectory = Path.GetDirectoryName(assemblyPath);
            var jsonPath = Path.Combine(assemblyDirectory!, "testconf.json");
            var json = JObject.Parse(File.ReadAllText(jsonPath));
            bootstrapServers = json["bootstrapServers"]!.ToString();

            SinglePartitionTopic = "dotnet_test_" + Guid.NewGuid().ToString();
            PartitionedTopic = "dotnet_test_" + Guid.NewGuid().ToString();

            // Create shared topics that are used by many of the tests.
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                adminClient.CreateTopicsAsync(new List<TopicSpecification>
                {
                    new TopicSpecification { Name = SinglePartitionTopic, NumPartitions = 1, ReplicationFactor = 1 },
                    new TopicSpecification { Name = PartitionedTopic, NumPartitions = partitionedTopicNumPartitions, ReplicationFactor = 1 }
                }).Wait();
            }
        }

        public void Dispose()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                adminClient.DeleteTopicsAsync(new List<string> { SinglePartitionTopic, PartitionedTopic }).Wait();
            }
        }

        public string SinglePartitionTopic { get; set; }

        public string PartitionedTopic { get; set; }
    }

    [CollectionDefinition("Global collection")]
    public class GlobalCollection : ICollectionFixture<GlobalFixture>
    {
        // This class has no code, and is never created. Its purpose is
        // simply to be the place to apply [CollectionDefinition] and all
        // the ICollectionFixture<> interfaces.
    }


    [Collection("Global collection")]
    public partial class Tests
    {
        private string singlePartitionTopic;
        private string partitionedTopic;

        private static List<object[]> kafkaParameters = null!;

        public Tests(GlobalFixture globalFixture)
        {
            singlePartitionTopic = globalFixture.SinglePartitionTopic;
            partitionedTopic = globalFixture.PartitionedTopic;
        }

        public static IEnumerable<object[]> KafkaParameters()
        {
            if (kafkaParameters == null)
            {
                var assemblyPath = typeof(Tests).GetTypeInfo().Assembly.Location;
                var assemblyDirectory = Path.GetDirectoryName(assemblyPath);
                var jsonPath = Path.Combine(assemblyDirectory!, "testconf.json");
                var json = JObject.Parse(File.ReadAllText(jsonPath));
                kafkaParameters = new List<object[]>
                {
                    new object[] { json["bootstrapServers"]!.ToString() }
                };
            }

            return kafkaParameters;
        }

        public static IEnumerable<object[]> KafkaProducersParameters()
        {
            foreach (var kafkaParameter in KafkaParameters())
            {
                yield return kafkaParameter.Append(TestProducerType.KeyValue).ToArray();
                yield return kafkaParameter.Append(TestProducerType.Binary).ToArray();
            }
        }
    }
}