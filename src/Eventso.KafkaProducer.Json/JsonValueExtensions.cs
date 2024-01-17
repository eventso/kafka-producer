using System.Text;
using CommunityToolkit.HighPerformance.Buffers;
using Confluent.Kafka;
using Timestamp = Confluent.Kafka.Timestamp;

namespace Eventso.KafkaProducer.Json;

/// <summary>
/// Extends binary producer with frequently used key types
/// </summary>
public static class JsonValueExtensions
{
    public static Task<DeliveryResult> ProduceJsonAsync<T>(
        this IProducer producer,
        string topic,
        ReadOnlySpan<byte> key,
        T value,
        IBuffer<byte>? buffer = null,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        return producer.ProduceAsync<JsonValue<T>>(topic, key, new(value), buffer ?? newBuffer!, cancellationToken, headers, timestamp,
            partition);
    }

    public static void ProduceJson<T>(
        this IProducer producer,
        string topic,
        ReadOnlySpan<byte> key,
        T value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        producer.Produce<JsonValue<T>>(topic, key, new(value), buffer ?? newBuffer!, headers, timestamp, deliveryHandler, partition);
    }

    public static void ProduceJson<T>(
        this MessageBatch batch,
        ReadOnlySpan<byte> key,
        T value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => batch.Produce<JsonValue<T>>(key, new(value), buffer ?? batch.GetBuffer(), headers, timestamp, partition);

    public static Task<DeliveryResult> ProduceJsonAsync<T>(
        this IProducer producer,
        string topic,
        short key,
        T value,
        IBuffer<byte>? buffer = null,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        return producer.ProduceAsync<ShortValue, JsonValue<T>>(topic, key, new(value), buffer ?? newBuffer!, cancellationToken, headers,
            timestamp, partition);
    }

    public static void ProduceJson<T>(
        this IProducer producer,
        string topic,
        short key,
        T value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        producer.Produce<ShortValue, JsonValue<T>>(topic, key, new(value), buffer ?? newBuffer!, headers, timestamp, deliveryHandler,
            partition);
    }

    public static void ProduceJson<T>(
        this MessageBatch batch,
        short key,
        T value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => batch.Produce<ShortValue, JsonValue<T>>(key, new(value), buffer ?? batch.GetBuffer(), headers, timestamp, partition);

    public static Task<DeliveryResult> ProduceJsonAsync<T>(
        this IProducer producer,
        string topic,
        int key,
        T value,
        IBuffer<byte>? buffer = null,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        return producer.ProduceAsync<IntValue, JsonValue<T>>(topic, key, new(value), buffer ?? newBuffer!, cancellationToken, headers,
            timestamp, partition);
    }

    public static void ProduceJson<T>(
        this IProducer producer,
        string topic,
        int key,
        T value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        producer.Produce<IntValue, JsonValue<T>>(topic, key, new(value), buffer ?? newBuffer!, headers, timestamp, deliveryHandler,
            partition);
    }

    public static void ProduceJson<T>(
        this MessageBatch batch,
        int key,
        T value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => batch.Produce<IntValue, JsonValue<T>>(key, new(value), buffer ?? batch.GetBuffer(), headers, timestamp, partition);

    public static Task<DeliveryResult> ProduceJsonAsync<T>(
        this IProducer producer,
        string topic,
        long key,
        T value,
        IBuffer<byte>? buffer = null,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        return producer.ProduceAsync<LongValue, JsonValue<T>>(topic, key, new(value), buffer ?? newBuffer!, cancellationToken, headers,
            timestamp, partition);
    }

    public static void ProduceJson<T>(
        this IProducer producer,
        string topic,
        long key,
        T value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        producer.Produce<LongValue, JsonValue<T>>(topic, key, new(value), buffer ?? newBuffer!, headers, timestamp, deliveryHandler,
            partition);
    }

    public static void ProduceJson<T>(
        this MessageBatch batch,
        long key,
        T value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => batch.Produce<LongValue, JsonValue<T>>(key, new(value), buffer ?? batch.GetBuffer(), headers, timestamp, partition);

    public static Task<DeliveryResult> ProduceJsonAsync<T>(
        this IProducer producer,
        string topic,
        string? key,
        T value,
        IBuffer<byte>? buffer = null,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null,
        Encoding? keyEncoding = default)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        return producer.ProduceAsync<StringValue, JsonValue<T>>(
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

    public static void ProduceJson<T>(
        this IProducer producer,
        string topic,
        string? key,
        T value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null,
        Encoding? keyEncoding = default)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        producer.Produce<StringValue, JsonValue<T>>(
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

    public static void ProduceJson<T>(
        this MessageBatch batch,
        string? key,
        T value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null,
        Encoding? keyEncoding = default)
        => batch.Produce<StringValue, JsonValue<T>>(
            keyEncoding == null ? new(key) : new(key, keyEncoding),
            new(value),
            buffer ?? batch.GetBuffer(),
            headers,
            timestamp,
            partition);


    /// Converts Guid to bytes with big endian bytes ordering  
    public static Task<DeliveryResult> ProduceJsonAsync<T>(
        this IProducer producer,
        string topic,
        Guid key,
        T value,
        IBuffer<byte>? buffer = null,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        return producer.ProduceAsync<GuidValue, JsonValue<T>>(topic, key, new(value), buffer ?? newBuffer!, cancellationToken, headers,
            timestamp, partition);
    }


    /// Converts Guid to bytes with big endian bytes ordering  
    public static void ProduceJson<T>(
        this IProducer producer,
        string topic,
        Guid key,
        T value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        producer.Produce<GuidValue, JsonValue<T>>(topic, key, new(value), buffer ?? newBuffer!, headers, timestamp, deliveryHandler,
            partition);
    }

    /// Converts Guid to bytes with big endian bytes ordering  
    public static void ProduceJson<T>(
        this MessageBatch batch,
        Guid key,
        T value,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => batch.Produce<GuidValue, JsonValue<T>>(key, new(value), buffer ?? batch.GetBuffer(), headers, timestamp, partition);
}