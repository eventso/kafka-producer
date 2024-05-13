using System.Text;
using System.Text.Json.Serialization;
using CommunityToolkit.HighPerformance.Buffers;
using Confluent.Kafka;
using Timestamp = Confluent.Kafka.Timestamp;

namespace Eventso.KafkaProducer;

/// <summary>
/// Extends binary producer with frequently used key types
/// </summary>
public static class JsonContextValueExtensions
{
    public static Task<DeliveryResult> ProduceJsonAsync<T>(
        this IProducer producer,
        string topic,
        ReadOnlySpan<byte> key,
        T value,
        JsonSerializerContext serializerContext,
        IBuffer<byte>? buffer = null,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        return producer.ProduceAsync<JsonContextValue<T>>(
            topic, 
            key, 
            new(value, serializerContext), 
            buffer ?? newBuffer!, 
            cancellationToken, 
            headers, 
            timestamp,
            partition);
    }

    public static void ProduceJson<T>(
        this IProducer producer,
        string topic,
        ReadOnlySpan<byte> key,
        T value,
        JsonSerializerContext serializerContext,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        producer.Produce<JsonContextValue<T>>(
            topic,
            key,
            new(value, serializerContext),
            buffer ?? newBuffer!,
            headers,
            timestamp,
            deliveryHandler,
            partition);
    }

    public static void ProduceJson<T>(
        this MessageBatch batch,
        ReadOnlySpan<byte> key,
        T value,
        JsonSerializerContext serializerContext,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => batch.Produce<JsonContextValue<T>>(
            key,
            new(value, serializerContext),
            buffer ?? batch.GetBuffer(),
            headers,
            timestamp,
            partition);

    public static Task<DeliveryResult> ProduceJsonAsync<T>(
        this IProducer producer,
        string topic,
        short key,
        T value,
        JsonSerializerContext serializerContext,
        IBuffer<byte>? buffer = null,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        return producer.ProduceAsync<ShortValue, JsonContextValue<T>>(
            topic,
            key,
            new(value, serializerContext),
            buffer ?? newBuffer!,
            cancellationToken,
            headers,
            timestamp,
            partition);
    }

    public static void ProduceJson<T>(
        this IProducer producer,
        string topic,
        short key,
        T value,
        JsonSerializerContext serializerContext,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        producer.Produce<ShortValue, JsonContextValue<T>>(
            topic,
            key,
            new(value, serializerContext),
            buffer ?? newBuffer!,
            headers,
            timestamp,
            deliveryHandler,
            partition);
    }

    public static void ProduceJson<T>(
        this MessageBatch batch,
        short key,
        T value,
        JsonSerializerContext serializerContext,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => batch.Produce<ShortValue, JsonContextValue<T>>(
            key,
            new(value, serializerContext),
            buffer ?? batch.GetBuffer(),
            headers,
            timestamp,
            partition);

    public static Task<DeliveryResult> ProduceJsonAsync<T>(
        this IProducer producer,
        string topic,
        int key,
        T value,
        JsonSerializerContext serializerContext,
        IBuffer<byte>? buffer = null,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        return producer.ProduceAsync<IntValue, JsonContextValue<T>>(
            topic,
            key,
            new(value, serializerContext),
            buffer ?? newBuffer!,
            cancellationToken,
            headers,
            timestamp,
            partition);
    }

    public static void ProduceJson<T>(
        this IProducer producer,
        string topic,
        int key,
        T value,
        JsonSerializerContext serializerContext,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null ? new ArrayPoolBufferWriter<byte>() : null;

        producer.Produce<IntValue, JsonContextValue<T>>(
            topic,
            key,
            new(value, serializerContext),
            buffer ?? newBuffer!,
            headers,
            timestamp,
            deliveryHandler,
            partition);
    }

    public static void ProduceJson<T>(
        this MessageBatch batch,
        int key,
        T value,
        JsonSerializerContext serializerContext,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => batch.Produce<IntValue, JsonContextValue<T>>(
            key,
            new(value, serializerContext),
            buffer ?? batch.GetBuffer(),
            headers,
            timestamp,
            partition);

    public static Task<DeliveryResult> ProduceJsonAsync<T>(
        this IProducer producer,
        string topic,
        long key,
        T value,
        JsonSerializerContext serializerContext,
        IBuffer<byte>? buffer = null,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null 
            ? new ArrayPoolBufferWriter<byte>()
            : null;

        return producer.ProduceAsync<LongValue, JsonContextValue<T>>(
            topic,
            key,
            new(value, serializerContext),
            buffer ?? newBuffer!,
            cancellationToken,
            headers,
            timestamp,
            partition);
    }

    public static void ProduceJson<T>(
        this IProducer producer,
        string topic,
        long key,
        T value,
        JsonSerializerContext serializerContext,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null
            ? new ArrayPoolBufferWriter<byte>()
            : null;

        producer.Produce<LongValue, JsonContextValue<T>>(
            topic,
            key,
            new(value, serializerContext),
            buffer ?? newBuffer!,
            headers,
            timestamp,
            deliveryHandler,
            partition);
    }

    public static void ProduceJson<T>(
        this MessageBatch batch,
        long key,
        T value,
        JsonSerializerContext serializerContext,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => batch.Produce<LongValue, JsonContextValue<T>>(
            key,
            new(value, serializerContext),
            buffer ?? batch.GetBuffer(),
            headers,
            timestamp,
            partition);

    public static Task<DeliveryResult> ProduceJsonAsync<T>(
        this IProducer producer,
        string topic,
        string? key,
        T value,
        JsonSerializerContext serializerContext,
        IBuffer<byte>? buffer = null,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null,
        Encoding? keyEncoding = default)
    {
        using var newBuffer = buffer == null
            ? new ArrayPoolBufferWriter<byte>()
            : null;

        return producer.ProduceAsync<StringValue, JsonContextValue<T>>(
            topic,
            keyEncoding == null
                ? new(key)
                : new(key,
                    keyEncoding),
            new(value, serializerContext),
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
        JsonSerializerContext serializerContext,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null,
        Encoding? keyEncoding = default)
    {
        using var newBuffer = buffer == null
            ? new ArrayPoolBufferWriter<byte>()
            : null;

        producer.Produce<StringValue, JsonContextValue<T>>(
            topic,
            keyEncoding == null
                ? new(key)
                : new(key,
                    keyEncoding),
            new(value, serializerContext),
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
        JsonSerializerContext serializerContext,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null,
        Encoding? keyEncoding = default)
        => batch.Produce<StringValue, JsonContextValue<T>>(
            keyEncoding == null
                ? new(key)
                : new(key,
                    keyEncoding),
            new(value, serializerContext),
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
        JsonSerializerContext serializerContext,
        IBuffer<byte>? buffer = null,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null
            ? new ArrayPoolBufferWriter<byte>()
            : null;

        return producer.ProduceAsync<GuidValue, JsonContextValue<T>>(
            topic,
            key,
            new(value, serializerContext),
            buffer ?? newBuffer!,
            cancellationToken,
            headers,
            timestamp,
            partition);
    }


    /// Converts Guid to bytes with big endian bytes ordering  
    public static void ProduceJson<T>(
        this IProducer producer,
        string topic,
        Guid key,
        T value,
        JsonSerializerContext serializerContext,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
    {
        using var newBuffer = buffer == null
            ? new ArrayPoolBufferWriter<byte>()
            : null;

        producer.Produce<GuidValue, JsonContextValue<T>>(
            topic,
            key,
            new(value, serializerContext),
            buffer ?? newBuffer!,
            headers,
            timestamp,
            deliveryHandler,
            partition);
    }

    /// Converts Guid to bytes with big endian bytes ordering  
    public static void ProduceJson<T>(
        this MessageBatch batch,
        Guid key,
        T value,
        JsonSerializerContext serializerContext,
        IBuffer<byte>? buffer = null,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => batch.Produce<GuidValue, JsonContextValue<T>>(
            key,
            new(value, serializerContext),
            buffer ?? batch.GetBuffer(),
            headers,
            timestamp,
            partition);
}