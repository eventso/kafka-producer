using System.Text;
using CommunityToolkit.HighPerformance.Buffers;
using Confluent.Kafka;
using Google.Protobuf;
using Timestamp = Confluent.Kafka.Timestamp;

namespace Eventso.KafkaProducer;

public static class ProtoValueBufferExtensions
{
    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        ReadOnlySpan<byte> key,
        IMessage value,
        IBuffer<byte> buffer,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => producer.ProduceAsync<ProtoBufferValue>(topic, key, new(value), buffer, cancellationToken, headers, timestamp, partition);

    public static void Produce(
        this IProducer producer,
        string topic,
        ReadOnlySpan<byte> key,
        IMessage value,
        IBuffer<byte> buffer,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
        => producer.Produce<ProtoBufferValue>(topic, key, new(value), buffer, headers, timestamp, deliveryHandler, partition);

    public static void Produce(
        this MessageBatch batch,
        ReadOnlySpan<byte> key,
        IMessage value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => batch.Produce<ProtoBufferValue>(key, new(value), buffer ?? batch.GetBuffer(), headers, timestamp, partition);

    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        short key,
        IMessage value,
        IBuffer<byte> buffer,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => producer.ProduceAsync<ShortValue, ProtoBufferValue>(topic, key, new(value), buffer, cancellationToken, headers, timestamp, partition);

    public static void Produce(
        this IProducer producer,
        string topic,
        short key,
        IMessage value,
        IBuffer<byte> buffer,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
        => producer.Produce<ShortValue, ProtoBufferValue>(topic, key, new(value), buffer, headers, timestamp, deliveryHandler, partition);

    public static void Produce(
        this MessageBatch batch,
        short key,
        IMessage value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => batch.Produce<ShortValue, ProtoBufferValue>(key, new(value), buffer ?? batch.GetBuffer(), headers, timestamp, partition);

    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        int key,
        IMessage value,
        IBuffer<byte> buffer,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => producer.ProduceAsync<IntValue, ProtoBufferValue>(topic, key, new(value), buffer, cancellationToken, headers, timestamp, partition);

    public static void Produce(
        this IProducer producer,
        string topic,
        int key,
        IMessage value,
        IBuffer<byte> buffer,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
        => producer.Produce<IntValue, ProtoBufferValue>(topic, key, new(value), buffer, headers, timestamp, deliveryHandler, partition);

    public static void Produce(
        this MessageBatch batch,
        int key,
        IMessage value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => batch.Produce<IntValue, ProtoBufferValue>(key, new(value), buffer ?? batch.GetBuffer(), headers, timestamp, partition);

    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        long key,
        IMessage value,
        IBuffer<byte> buffer,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => producer.ProduceAsync<LongValue, ProtoBufferValue>(topic, key, new(value), buffer, cancellationToken, headers, timestamp, partition);

    public static void Produce(
        this IProducer producer,
        string topic,
        long key,
        IMessage value,
        IBuffer<byte> buffer,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
        => producer.Produce<LongValue, ProtoBufferValue>(topic, key, new(value), buffer, headers, timestamp, deliveryHandler, partition);

    public static void Produce(
        this MessageBatch batch,
        long key,
        IMessage value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => batch.Produce<LongValue, ProtoBufferValue>(key, new(value), buffer ?? batch.GetBuffer(), headers, timestamp, partition);

    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        string? key,
        IMessage value,
        IBuffer<byte> buffer,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null,
        Encoding? keyEncoding = default)
        => producer.ProduceAsync<StringValue, ProtoBufferValue>(
            topic,
            keyEncoding == null
                ? new(key)
                : new(key, keyEncoding),
            new(value),
            buffer,
            cancellationToken,
            headers,
            timestamp,
            partition);

    public static void Produce(
        this IProducer producer,
        string topic,
        string? key,
        IMessage value,
        IBuffer<byte> buffer,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null,
        Encoding? keyEncoding = default)
        => producer.Produce<StringValue, ProtoBufferValue>(
            topic,
            keyEncoding == null
                ? new(key)
                : new(key, keyEncoding),
            new(value),
            buffer,
            headers,
            timestamp,
            deliveryHandler,
            partition);

    public static void Produce(
        this MessageBatch batch,
        string? key,
        IMessage value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null,
        Encoding? keyEncoding = default)
        => batch.Produce<StringValue, ProtoBufferValue>(
            keyEncoding == null ? new(key) : new(key, keyEncoding),
            new(value),
            buffer ?? batch.GetBuffer(),
            headers,
            timestamp,
            partition);

    /// Converts Guid to bytes with big endian bytes ordering  
    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        Guid key,
        IMessage value,
        IBuffer<byte> buffer,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => producer.ProduceAsync<GuidValue, ProtoBufferValue>(topic, key, new(value), buffer, cancellationToken, headers, timestamp, partition);

    /// Converts Guid to bytes with big endian bytes ordering  
    public static void Produce(
        this IProducer producer,
        string topic,
        Guid key,
        IMessage value,
        IBuffer<byte> buffer,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
        => producer.Produce<GuidValue, ProtoBufferValue>(topic, key, new(value), buffer, headers, timestamp, deliveryHandler, partition);

    /// Converts Guid to bytes with big endian bytes ordering  
    public static void Produce(
        this MessageBatch batch,
        Guid key,
        IMessage value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => batch.Produce<GuidValue, ProtoBufferValue>(key, new(value), buffer ?? batch.GetBuffer(), headers, timestamp, partition);
}