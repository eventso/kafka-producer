using Confluent.Kafka;

namespace Eventso.KafkaProducer;

/// <summary>
/// Extends binary producer with frequently used key types
/// </summary>
public static class ProducerExtensions
{
    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        TopicPartition topicPartition,
        ReadOnlySpan<byte> key,
        ReadOnlySpan<byte> value,
        CancellationToken cancellationToken = default(CancellationToken),
        Headers? headers = null,
        Timestamp timestamp = default)
    {
        return producer.ProduceAsync(
            topicPartition.Topic,
            key,
            value,
            cancellationToken,
            headers,
            timestamp,
            topicPartition.Partition);
    }

    public static void Produce(
        this IProducer producer,
        TopicPartition topicPartition,
        ReadOnlySpan<byte> key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null)
    {
        producer.Produce(
            topicPartition.Topic,
            key,
            value,
            headers,
            timestamp,
            deliveryHandler,
            topicPartition.Partition);
    }
}