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

#pragma warning disable xUnit1026

using Confluent.Kafka;
using Xunit;

namespace Eventso.KafkaProducer.IntegrationTests.Tests
{
    public partial class Tests
    {
        /// <summary>
        ///     Tests that ObjectDisposedException is thrown rather than AccessViolationException
        ///     when Dispose has been called
        /// </summary>
        [Theory, MemberData(nameof(KafkaProducersParameters))]
        public void Producer_ClosedHandle(string bootstrapServers, TestProducerType producerType)
        {
            LogToFile("start Producer_ClosedHandle");

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                EnableBackgroundPoll = false
            };
            var producer = new TestProducerBuilder<Null, Null>(producerConfig, producerType).Build();
            producer.Poll(TimeSpan.FromMilliseconds(10));
            producer.Dispose();
            Assert.Throws<ObjectDisposedException>(() => producer.Poll(TimeSpan.FromMilliseconds(10)));

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Producer_ClosedHandle");
        }
    }
}