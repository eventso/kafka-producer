using Confluent.Kafka;

namespace Eventso.KafkaProducer;

public static class ProducerExtensions
{
    public static MessageBatch CreateBatch(this IProducer producer, string topic)
        => new MessageBatch(producer, topic);

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