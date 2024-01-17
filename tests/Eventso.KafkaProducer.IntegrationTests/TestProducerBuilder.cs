using Confluent.Kafka;
using Google.Protobuf;
using Tests;

namespace Eventso.KafkaProducer.IntegrationTests
{
    public enum TestProducerType
    {
        KeyValue,
        Binary
    }

    internal sealed class TestProducerBuilder<TKey, TValue> : ProducerBuilder<TKey, TValue>
    {
        private readonly TestProducerType producerType;

        public TestProducerBuilder(
            IEnumerable<KeyValuePair<string, string>> config,
            TestProducerType producerType) : base(config)
        {
            this.producerType = producerType;
        }

        public override IProducer<TKey, TValue> Build()
        {
            if (producerType == TestProducerType.KeyValue)
                return base.Build();

            var builder = new ProducerBuilder(this.Config);
            builder.SetDefaultPartitioner(this.DefaultPartitioner);

            foreach (var (topic, partitioner) in this.Partitioners)
                builder.SetPartitioner(topic, partitioner);

            builder.SetErrorHandler((p, e) => this.ErrorHandler?.Invoke(new DependentProducerBuilder<TKey, TValue>(p.Handle).Build(), e));
            builder.SetLogHandler((p, m) => this.LogHandler?.Invoke(new DependentProducerBuilder<TKey, TValue>(p.Handle).Build(), m));
            
            builder.SetOAuthBearerTokenRefreshHandler((p, t) =>
                this.OAuthBearerTokenRefreshHandler?.Invoke(new DependentProducerBuilder<TKey, TValue>(p.Handle).Build(), t));
            
            builder.SetStatisticsHandler((p, s) =>
                this.StatisticsHandler?.Invoke(new DependentProducerBuilder<TKey, TValue>(p.Handle).Build(), s));

            return new TestProducerAdapter<TKey, TValue>(builder.Build(), KeySerializer, ValueSerializer,
                AsyncKeySerializer, AsyncValueSerializer);
        }
    }
}