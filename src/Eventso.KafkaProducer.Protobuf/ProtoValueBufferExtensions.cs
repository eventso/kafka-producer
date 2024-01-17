using System.Text;
using CommunityToolkit.HighPerformance.Buffers;
using Confluent.Kafka;
using Google.Protobuf;
using Timestamp = Confluent.Kafka.Timestamp;

namespace Eventso.KafkaProducer.Protobuf;

public static class ProtoValueBufferExtensions
{
    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        ReadOnlySpan<byte> key,
        IMessage value,
        IBuffer<byte>? buffer = null,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        return producer.ProduceAsync<ProtoValue>(topic, key, new(value), buffer ?? newBuffer!, cancellationToken, headers, timestamp,
            partition);
    }

    public static void Produce(
        this IProducer producer,
        string topic,
        ReadOnlySpan<byte> key,
        IMessage value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        producer.Produce<ProtoValue>(topic, key, new(value), buffer ?? newBuffer!, headers, timestamp, deliveryHandler, partition);
    }

    public static void Produce(
        this MessageBatch batch,
        ReadOnlySpan<byte> key,
        IMessage value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => batch.Produce<ProtoValue>(key, new(value), buffer ?? batch.GetBuffer(), headers, timestamp, partition);

    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        short key,
        IMessage value,
        IBuffer<byte>? buffer = null,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        return producer.ProduceAsync<ShortValue, ProtoValue>(topic, key, new(value), buffer ?? newBuffer!, cancellationToken, headers,
            timestamp, partition);
    }

    public static void Produce(
        this IProducer producer,
        string topic,
        short key,
        IMessage value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        producer.Produce<ShortValue, ProtoValue>(topic, key, new(value), buffer ?? newBuffer!, headers, timestamp, deliveryHandler,
            partition);
    }

    public static void Produce(
        this MessageBatch batch,
        short key,
        IMessage value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => batch.Produce<ShortValue, ProtoValue>(key, new(value), buffer ?? batch.GetBuffer(), headers, timestamp, partition);

    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        int key,
        IMessage value,
        IBuffer<byte>? buffer = null,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        return producer.ProduceAsync<IntValue, ProtoValue>(topic, key, new(value), buffer ?? newBuffer!, cancellationToken, headers,
            timestamp, partition);
    }

    public static void Produce(
        this IProducer producer,
        string topic,
        int key,
        IMessage value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        producer.Produce<IntValue, ProtoValue>(topic, key, new(value), buffer ?? newBuffer!, headers, timestamp, deliveryHandler,
            partition);
    }

    public static void Produce(
        this MessageBatch batch,
        int key,
        IMessage value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => batch.Produce<IntValue, ProtoValue>(key, new(value), buffer ?? batch.GetBuffer(), headers, timestamp, partition);

    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        long key,
        IMessage value,
        IBuffer<byte>? buffer = null,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        return producer.ProduceAsync<LongValue, ProtoValue>(topic, key, new(value), buffer ?? newBuffer!, cancellationToken, headers,
            timestamp, partition);
    }

    public static void Produce(
        this IProducer producer,
        string topic,
        long key,
        IMessage value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        producer.Produce<LongValue, ProtoValue>(topic, key, new(value), buffer ?? newBuffer!, headers, timestamp, deliveryHandler,
            partition);
    }

    public static void Produce(
        this MessageBatch batch,
        long key,
        IMessage value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => batch.Produce<LongValue, ProtoValue>(key, new(value), buffer ?? batch.GetBuffer(), headers, timestamp, partition);

    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        string? key,
        IMessage value,
        IBuffer<byte>? buffer = null,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null,
        Encoding? keyEncoding = default)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        return producer.ProduceAsync<StringValue, ProtoValue>(
            topic,
            keyEncoding == null
                ? new(key)
                : new(key, keyEncoding),
            new(value),
            buffer ?? newBuffer!,
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
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null,
        Encoding? keyEncoding = default)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        producer.Produce<StringValue, ProtoValue>(
            topic,
            keyEncoding == null
                ? new(key)
                : new(key, keyEncoding),
            new(value),
            buffer ?? newBuffer!,
            headers,
            timestamp,
            deliveryHandler,
            partition);
    }

    public static void Produce(
        this MessageBatch batch,
        string? key,
        IMessage value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null,
        Encoding? keyEncoding = default)
        => batch.Produce<StringValue, ProtoValue>(
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
        IBuffer<byte>? buffer = null,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        return producer.ProduceAsync<GuidValue, ProtoValue>(topic, key, new(value), buffer ?? newBuffer!, cancellationToken, headers,
            timestamp, partition);
    }


    /// Converts Guid to bytes with big endian bytes ordering  
    public static void Produce(
        this IProducer producer,
        string topic,
        Guid key,
        IMessage value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        producer.Produce<GuidValue, ProtoValue>(topic, key, new(value), buffer ?? newBuffer!, headers, timestamp, deliveryHandler,
            partition);
    }

    /// Converts Guid to bytes with big endian bytes ordering  
    public static void Produce(
        this MessageBatch batch,
        Guid key,
        IMessage value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => batch.Produce<GuidValue, ProtoValue>(key, new(value), buffer ?? batch.GetBuffer(), headers, timestamp, partition);
}