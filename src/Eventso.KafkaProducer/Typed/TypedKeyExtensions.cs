using System.Buffers;
using System.Text;
using Confluent.Kafka;

namespace Eventso.KafkaProducer;

/// <summary>
/// Extends binary producer with frequently used key types
/// </summary>
public static class TypedKeyExtensions
{
    private const int StackThreshold = 256;

    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        short key,
        ReadOnlySpan<byte> value,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => producer.ProduceAsync<ShortValue>(topic, key, value, cancellationToken, headers, timestamp, partition);

    public static void Produce(
        this IProducer producer,
        string topic,
        short key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
        => Produce<ShortValue>(producer, topic, key, value, headers, timestamp, deliveryHandler, partition);

    public static void Produce(
        this MessageBatch batch,
        short key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => Produce<ShortValue>(batch, key, value, headers, timestamp, partition);

    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        int key,
        ReadOnlySpan<byte> value,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => producer.ProduceAsync<IntValue>(topic, key, value, cancellationToken, headers, timestamp, partition);

    public static void Produce(
        this IProducer producer,
        string topic,
        int key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
        => Produce<IntValue>(producer, topic, key, value, headers, timestamp, deliveryHandler, partition);

    public static void Produce(
        this MessageBatch batch,
        int key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => Produce<IntValue>(batch, key, value, headers, timestamp, partition);

    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        long key,
        ReadOnlySpan<byte> value,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => producer.ProduceAsync<LongValue>(topic, key, value, cancellationToken, headers, timestamp, partition);

    public static void Produce(
        this IProducer producer,
        string topic,
        long key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
        => Produce<LongValue>(producer, topic, key, value, headers, timestamp, deliveryHandler, partition);

    public static void Produce(
        this MessageBatch batch,
        long key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => Produce<LongValue>(batch, key, value, headers, timestamp, partition);

    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        string? key,
        ReadOnlySpan<byte> value,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null,
        Encoding? keyEncoding = default)
    {
        return ProduceAsync<StringValue>(
            producer,
            topic,
            keyEncoding == null
                ? new(key)
                : new(key, keyEncoding),
            value,
            cancellationToken,
            headers,
            timestamp,
            partition);
    }

    public static void Produce(
        this IProducer producer,
        string topic,
        string? key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null,
        Encoding? keyEncoding = default)
    {
        Produce<StringValue>(
            producer,
            topic,
            keyEncoding == null
                ? new(key)
                : new(key, keyEncoding),
            value,
            headers,
            timestamp,
            deliveryHandler,
            partition);
    }

    public static void Produce(
        this MessageBatch batch,
        string? key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null,
        Encoding? keyEncoding = default)
        => Produce<StringValue>(batch, keyEncoding == null ? new(key) : new(key, keyEncoding), value, headers, timestamp, partition);


    /// Converts Guid to bytes with big endian bytes ordering  
    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        Guid key,
        ReadOnlySpan<byte> value,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => producer.ProduceAsync<GuidValue>(topic, key, value, cancellationToken, headers, timestamp, partition);


    /// Converts Guid to bytes with big endian bytes ordering  
    public static void Produce(
        this IProducer producer,
        string topic,
        Guid key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
        => Produce<GuidValue>(producer, topic, key, value, headers, timestamp, deliveryHandler, partition);

    /// Converts Guid to bytes with big endian bytes ordering  
    public static void Produce(
        this MessageBatch batch,
        Guid key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => Produce<GuidValue>(batch, key, value, headers, timestamp, partition);

    private static Task<DeliveryResult> ProduceAsync<TKey>(
        this IProducer producer,
        string topic,
        TKey key,
        ReadOnlySpan<byte> value,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        where TKey : IBinarySerializable
    {
        var keySize = key.GetSize();
        var keyBytesPooled = keySize <= StackThreshold ? null : ArrayPool<byte>.Shared.Rent(keySize);
        var keyBytes = keySize == 0 ? Span<byte>.Empty : keyBytesPooled ?? stackalloc byte[keySize];

        try
        {
            var bytesWritten = key.WriteBytes(keyBytes);

            return producer.ProduceAsync(topic, keyBytes.Slice(0, bytesWritten), value, cancellationToken, headers, timestamp, partition);
        }
        finally
        {
            if (keyBytesPooled != null)
                ArrayPool<byte>.Shared.Return(keyBytesPooled);
        }
    }

    private static void Produce<TKey>(
        this IProducer producer,
        string topic,
        TKey key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
        where TKey : IBinarySerializable
    {
        var keySize = key.GetSize();
        var keyBytesPooled = keySize <= StackThreshold ? null : ArrayPool<byte>.Shared.Rent(keySize);
        var keyBytes = keySize == 0 ? Span<byte>.Empty : keyBytesPooled ?? stackalloc byte[keySize];

        try
        {
            var bytesWritten = key.WriteBytes(keyBytes);

            producer.Produce(topic, keyBytes.Slice(0, bytesWritten), value, headers, timestamp, deliveryHandler, partition);
        }
        finally
        {
            if (keyBytesPooled != null)
                ArrayPool<byte>.Shared.Return(keyBytesPooled);
        }
    }

    private static void Produce<TKey>(
        this MessageBatch batch,
        TKey key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        where TKey : IBinarySerializable
    {
        var keySize = key.GetSize();
        var keyBytesPooled = keySize <= StackThreshold ? null : ArrayPool<byte>.Shared.Rent(keySize);
        var keyBytes = keySize == 0 ? Span<byte>.Empty : keyBytesPooled ?? stackalloc byte[keySize];

        try
        {
            var bytesWritten = key.WriteBytes(keyBytes);

            batch.Produce(keyBytes.Slice(0, bytesWritten), value, headers, timestamp, partition);
        }
        finally
        {
            if (keyBytesPooled != null)
                ArrayPool<byte>.Shared.Return(keyBytesPooled);
        }
    }
}