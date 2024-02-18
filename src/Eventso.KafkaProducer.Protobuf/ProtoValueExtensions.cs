using System.Text;
using Confluent.Kafka;
using Google.Protobuf;
using Timestamp = Confluent.Kafka.Timestamp;

namespace Eventso.KafkaProducer;

public static class ProtoValueExtensions
{
    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        ReadOnlySpan<byte> key,
        IMessage value,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => producer.ProduceAsync<ProtoValue>(topic, key, new(value), cancellationToken, headers, timestamp, partition);

    public static void Produce(
        this IProducer producer,
        string topic,
        ReadOnlySpan<byte> key,
        IMessage value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
        => producer.Produce<ProtoValue>(topic, key, new(value), headers, timestamp, deliveryHandler, partition);

    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        short key,
        IMessage value,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => producer.ProduceAsync<ShortValue, ProtoValue>(topic, key, new(value), cancellationToken, headers, timestamp, partition);

    public static void Produce(
        this IProducer producer,
        string topic,
        short key,
        IMessage value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
        => producer.Produce<ShortValue, ProtoValue>(topic, key, new(value), headers, timestamp, deliveryHandler, partition);

    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        int key,
        IMessage value,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => producer.ProduceAsync<IntValue, ProtoValue>(topic, key, new(value), cancellationToken, headers, timestamp, partition);

    public static void Produce(
        this IProducer producer,
        string topic,
        int key,
        IMessage value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
        => producer.Produce<IntValue, ProtoValue>(topic, key, new(value), headers, timestamp, deliveryHandler, partition);

    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        long key,
        IMessage value,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => producer.ProduceAsync<LongValue, ProtoValue>(topic, key, new(value), cancellationToken, headers, timestamp, partition);

    public static void Produce(
        this IProducer producer,
        string topic,
        long key,
        IMessage value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
        => producer.Produce<LongValue, ProtoValue>(topic, key, new(value), headers, timestamp, deliveryHandler, partition);

    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        string? key,
        IMessage value,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null,
        Encoding? keyEncoding = default)
    {
        return producer.ProduceAsync<StringValue, ProtoValue>(
            topic,
            keyEncoding == null
                ? new(key)
                : new(key, keyEncoding),
            new(value),
            cancellationToken,
            headers,
            timestamp,
            partition);
    }

    public static void Produce(
        this IProducer producer,
        string topic,
        string? key,
        IMessage value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null,
        Encoding? keyEncoding = default)
    {
        producer.Produce<StringValue, ProtoValue>(
            topic,
            keyEncoding == null
                ? new(key)
                : new(key, keyEncoding),
            new(value),
            headers,
            timestamp,
            deliveryHandler,
            partition);
    }

    /// Converts Guid to bytes with big endian bytes ordering  
    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        Guid key,
        IMessage value,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => producer.ProduceAsync<GuidValue, ProtoValue>(topic, key, new(value), cancellationToken, headers, timestamp, partition);


    /// Converts Guid to bytes with big endian bytes ordering  
    public static void Produce(
        this IProducer producer,
        string topic,
        Guid key,
        IMessage value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
        => producer.Produce<GuidValue, ProtoValue>(topic, key, new(value), headers, timestamp, deliveryHandler, partition);
}