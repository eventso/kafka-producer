using Eventso.KafkaProducer;

// ReSharper disable once CheckNamespace
namespace Confluent.Kafka;

public static class ConfluentProducerBuilderExtensions
{
    public static IProducer BuildBinary<_, __>(this ProducerBuilder<_, __> builder)
    {
        var binaryBuilder = new ProducerBuilder(builder.Config);

        return binaryBuilder.Build();
    }
}