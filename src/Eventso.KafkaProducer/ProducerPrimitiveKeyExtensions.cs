using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Confluent.Kafka;

namespace Eventso.KafkaProducer;

/// <summary>
/// Extends binary producer with frequently used key types
/// </summary>
public static class ProducerPrimitiveKeyExtensions
{
    private const int StackThreshold = 256;

    /// <inheritdoc cref="IProducer.ProduceAsync" />
    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        short key,
        ReadOnlySpan<byte> value,
        CancellationToken cancellationToken = default(CancellationToken),
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        Span<byte> keyBytes = stackalloc byte[sizeof(short)];

        BinaryPrimitives.WriteInt16BigEndian(keyBytes, key);

        return producer.ProduceAsync(topic, keyBytes, value, cancellationToken, headers, timestamp, partition);
    }

    /// <inheritdoc cref="IProducer.Produce" />
    public static void Produce(
        this IProducer producer,
        string topic,
        short key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
    {
        Span<byte> keyBytes = stackalloc byte[sizeof(short)];

        BinaryPrimitives.WriteInt16BigEndian(keyBytes, key);

        producer.Produce(topic, keyBytes, value, headers, timestamp, deliveryHandler, partition);
    }

    /// <inheritdoc cref="IProducer.ProduceAsync" />
    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        int key,
        ReadOnlySpan<byte> value,
        CancellationToken cancellationToken = default(CancellationToken),
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        Span<byte> keyBytes = stackalloc byte[sizeof(int)];

        BinaryPrimitives.WriteInt32BigEndian(keyBytes, key);

        return producer.ProduceAsync(topic, keyBytes, value, cancellationToken, headers, timestamp, partition);
    }

    /// <inheritdoc cref="IProducer.Produce" />
    public static void Produce(
        this IProducer producer,
        string topic,
        int key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
    {
        Span<byte> keyBytes = stackalloc byte[sizeof(int)];

        BinaryPrimitives.WriteInt32BigEndian(keyBytes, key);

        producer.Produce(topic, keyBytes, value, headers, timestamp, deliveryHandler, partition);
    }

    /// <inheritdoc cref="IProducer.ProduceAsync" />
    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        long key,
        ReadOnlySpan<byte> value,
        CancellationToken cancellationToken = default(CancellationToken),
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        Span<byte> keyBytes = stackalloc byte[sizeof(long)];

        BinaryPrimitives.WriteInt64BigEndian(keyBytes, key);

        return producer.ProduceAsync(topic, keyBytes, value, cancellationToken, headers, timestamp, partition);
    }

    /// <inheritdoc cref="IProducer.Produce" />
    public static void Produce(
        this IProducer producer,
        string topic,
        long key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
    {
        Span<byte> keyBytes = stackalloc byte[sizeof(long)];

        BinaryPrimitives.WriteInt64BigEndian(keyBytes, key);

        producer.Produce(topic, keyBytes, value, headers, timestamp, deliveryHandler, partition);
    }

    /// <inheritdoc cref="IProducer.ProduceAsync" />
    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        string key,
        ReadOnlySpan<byte> value,
        CancellationToken cancellationToken = default(CancellationToken),
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null,
        Encoding? keyEncoding = default)
    {
        keyEncoding ??= Encoding.UTF8;
        var maxBytes = keyEncoding.GetMaxByteCount(key.Length);
        var allocOnStack = maxBytes <= StackThreshold;
        var keyBytesPooled = allocOnStack ? null : ArrayPool<byte>.Shared.Rent(maxBytes);

        var keyBytes = allocOnStack ? stackalloc byte[maxBytes] : keyBytesPooled;

        try
        {
            var bytesWritten = keyEncoding.GetBytes(key, keyBytes);

            return producer.ProduceAsync(topic, keyBytes.Slice(0, bytesWritten), value, cancellationToken, headers, timestamp, partition);
        }
        finally
        {
            if (keyBytesPooled != null)
                ArrayPool<byte>.Shared.Return(keyBytesPooled);
        }
    }

    /// <inheritdoc cref="IProducer.Produce" />
    public static void Produce(
        this IProducer producer,
        string topic,
        string key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null,
        Encoding? keyEncoding = default)
    {
        keyEncoding ??= Encoding.UTF8;
        var maxBytes = keyEncoding.GetMaxByteCount(key.Length);
        var allocOnStack = maxBytes <= StackThreshold;
        var keyBytesPooled = allocOnStack ? null : ArrayPool<byte>.Shared.Rent(maxBytes);

        var keyBytes = allocOnStack ? stackalloc byte[maxBytes] : keyBytesPooled;

        try
        {
            var bytesWritten = keyEncoding.GetBytes(key, keyBytes);

            producer.Produce(topic, keyBytes.Slice(0, bytesWritten), value, headers, timestamp, deliveryHandler, partition);
        }
        finally
        {
            if (keyBytesPooled != null)
                ArrayPool<byte>.Shared.Return(keyBytesPooled);
        }
    }

#if NET8_0_OR_GREATER

    /// Convert Guid to bytes with big endian bytes ordering  
    public static Task<DeliveryResult> ProduceAsync(
        this IProducer producer,
        string topic,
        Guid key,
        ReadOnlySpan<byte> value,
        CancellationToken cancellationToken = default(CancellationToken),
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        Span<byte> keyBytes = stackalloc byte[16];

        key.TryWriteBytes(keyBytes, bigEndian: true, out _);

        return producer.ProduceAsync(topic, keyBytes, value, cancellationToken, headers, timestamp, partition);
    }

    /// Convert Guid to bytes with big endian bytes ordering  
    public static void Produce(
        this IProducer producer,
        string topic,
        Guid key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
    {
        Span<byte> keyBytes = stackalloc byte[16];

        key.TryWriteBytes(keyBytes, bigEndian: true, out _);

        producer.Produce(topic, keyBytes, value, headers, timestamp, deliveryHandler, partition);
    }
#endif
}