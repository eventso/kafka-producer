using Eventso.KafkaProducer;

// ReSharper disable once CheckNamespace
namespace Confluent.Kafka;

public static class ConfluentProducerBuilderExtensions
{
    public static IProducer BuildBinary<_, __>(this ProducerBuilder<_, __> builder)
    {
        var producer = builder.Build();

        return producer.CreateBinary();
    }

    public static IProducer BuildBinary(this ProducerBuilder<byte[], byte[]> builder)
    {
        var binaryBuilder = new ProducerBuilder(builder);

        return binaryBuilder.Build();
    }
}