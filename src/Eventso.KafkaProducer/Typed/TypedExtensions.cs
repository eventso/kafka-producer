using System.Buffers;
using CommunityToolkit.HighPerformance.Buffers;
using Confluent.Kafka;

namespace Eventso.KafkaProducer;

public static class TypedExtensions
{
    private const int StackThreshold = 256;

    public static Task<DeliveryResult> ProduceAsync<TKey, TValue>(
        this IProducer producer,
        string topic,
        in TKey key,
        in TValue value,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        where TKey : IBinarySerializable
        where TValue : IBinarySerializable
    {
        var keySize = key.GetSize();
        var keyBytesPooled = keySize <= StackThreshold ? null : ArrayPool<byte>.Shared.Rent(keySize);
        var keyBytes = keySize == 0 ? Span<byte>.Empty : keyBytesPooled ?? stackalloc byte[keySize];

        var valueStackThreshold = StackThreshold + StackThreshold - keySize;
        var valueSize = value.GetSize();
        var valueBytesPooled = valueSize <= valueStackThreshold ? null : ArrayPool<byte>.Shared.Rent(valueSize);
        var valueBytes = valueSize == 0 ? Span<byte>.Empty : valueBytesPooled ?? stackalloc byte[valueSize];

        try
        {
            var keyBytesWritten = key.WriteBytes(keyBytes);
            var valueBytesWritten = value.WriteBytes(valueBytes);

            return producer.ProduceAsync(
                topic,
                keyBytes[..keyBytesWritten],
                valueBytes[..valueBytesWritten],
                cancellationToken,
                headers,
                timestamp,
                partition);
        }
        finally
        {
            if (keyBytesPooled != null)
                ArrayPool<byte>.Shared.Return(keyBytesPooled);

            if (valueBytesPooled != null)
                ArrayPool<byte>.Shared.Return(valueBytesPooled);
        }
    }

    public static void Produce<TKey, TValue>(
        this IProducer producer,
        string topic,
        in TKey key,
        in TValue value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
        where TKey : IBinarySerializable
        where TValue : IBinarySerializable
    {
        var keySize = key.GetSize();
        var keyBytesPooled = keySize <= StackThreshold ? null : ArrayPool<byte>.Shared.Rent(keySize);
        var keyBytes = keySize == 0 ? Span<byte>.Empty : keyBytesPooled ?? stackalloc byte[keySize];

        var valueStackThreshold = StackThreshold + StackThreshold - keySize;
        var valueSize = value.GetSize();
        var valueBytesPooled = valueSize <= valueStackThreshold ? null : ArrayPool<byte>.Shared.Rent(valueSize);
        var valueBytes = valueSize == 0 ? Span<byte>.Empty : valueBytesPooled ?? stackalloc byte[valueSize];

        try
        {
            var keyBytesWritten = key.WriteBytes(keyBytes);
            var valueBytesWritten = value.WriteBytes(valueBytes);

            producer.Produce(
                topic,
                keyBytes[..keyBytesWritten],
                valueBytes[..valueBytesWritten],
                headers,
                timestamp,
                deliveryHandler,
                partition);
        }
        finally
        {
            if (keyBytesPooled != null)
                ArrayPool<byte>.Shared.Return(keyBytesPooled);

            if (valueBytesPooled != null)
                ArrayPool<byte>.Shared.Return(valueBytesPooled);
        }
    }

    public static void Produce<TKey, TValue>(
        this MessageBatch batch,
        in TKey key,
        in TValue value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        where TKey : IBinarySerializable
        where TValue : IBinarySerializable
    {
        var keySize = key.GetSize();
        var keyBytesPooled = keySize <= StackThreshold ? null : ArrayPool<byte>.Shared.Rent(keySize);
        var keyBytes = keySize == 0 ? Span<byte>.Empty : keyBytesPooled ?? stackalloc byte[keySize];

        var valueStackThreshold = StackThreshold + StackThreshold - keySize;
        var valueSize = value.GetSize();
        var valueBytesPooled = valueSize <= valueStackThreshold ? null : ArrayPool<byte>.Shared.Rent(valueSize);
        var valueBytes = valueSize == 0 ? Span<byte>.Empty : valueBytesPooled ?? stackalloc byte[valueSize];

        try
        {
            var keyBytesWritten = key.WriteBytes(keyBytes);
            var valueBytesWritten = value.WriteBytes(valueBytes);

            batch.Produce(
                keyBytes[..keyBytesWritten],
                valueBytes[..valueBytesWritten],
                headers,
                timestamp,
                partition);
        }
        finally
        {
            if (keyBytesPooled != null)
                ArrayPool<byte>.Shared.Return(keyBytesPooled);

            if (valueBytesPooled != null)
                ArrayPool<byte>.Shared.Return(valueBytesPooled);
        }
    }


    public static Task<DeliveryResult> ProduceAsync<TKey, TValue>(
        this IProducer producer,
        string topic,
        in TKey key,
        in TValue value,
        IBuffer<byte> buffer,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        where TKey : IBinarySerializable
        where TValue : IBinaryBufferWritable
    {
        var keySize = key.GetSize();
        var keyBytesPooled = keySize <= StackThreshold ? null : ArrayPool<byte>.Shared.Rent(keySize);
        var keyBytes = keySize == 0 ? Span<byte>.Empty : keyBytesPooled ?? stackalloc byte[keySize];

        try
        {
            var keyBytesWritten = key.WriteBytes(keyBytes);

            buffer.Clear();
            value.WriteBytes(buffer);

            return producer.ProduceAsync(
                topic,
                keyBytes[..keyBytesWritten],
                buffer.WrittenSpan,
                cancellationToken,
                headers,
                timestamp,
                partition);
        }
        finally
        {
            if (keyBytesPooled != null)
                ArrayPool<byte>.Shared.Return(keyBytesPooled);
        }
    }

    public static void Produce<TKey, TValue>(
        this IProducer producer,
        string topic,
        in TKey key,
        in TValue value,
        IBuffer<byte> buffer,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
        where TKey : IBinarySerializable
        where TValue : IBinaryBufferWritable
    {
        var keySize = key.GetSize();
        var keyBytesPooled = keySize <= StackThreshold ? null : ArrayPool<byte>.Shared.Rent(keySize);
        var keyBytes = keySize == 0 ? Span<byte>.Empty : keyBytesPooled ?? stackalloc byte[keySize];

        try
        {
            var keyBytesWritten = key.WriteBytes(keyBytes);

            buffer.Clear();
            value.WriteBytes(buffer);

            producer.Produce(
                topic,
                keyBytes[..keyBytesWritten],
                buffer.WrittenSpan,
                headers,
                timestamp,
                deliveryHandler,
                partition);
        }
        finally
        {
            if (keyBytesPooled != null)
                ArrayPool<byte>.Shared.Return(keyBytesPooled);
        }
    }

    public static void Produce<TKey, TValue>(
        this MessageBatch batch,
        in TKey key,
        in TValue value,
        IBuffer<byte> buffer,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        where TKey : IBinarySerializable
        where TValue : IBinaryBufferWritable
    {
        var keySize = key.GetSize();
        var keyBytesPooled = keySize <= StackThreshold ? null : ArrayPool<byte>.Shared.Rent(keySize);
        var keyBytes = keySize == 0 ? Span<byte>.Empty : keyBytesPooled ?? stackalloc byte[keySize];

        try
        {
            var keyBytesWritten = key.WriteBytes(keyBytes);

            buffer.Clear();
            value.WriteBytes(buffer);

            batch.Produce(
                keyBytes[..keyBytesWritten],
                buffer.WrittenSpan,
                headers,
                timestamp,
                partition);
        }
        finally
        {
            if (keyBytesPooled != null)
                ArrayPool<byte>.Shared.Return(keyBytesPooled);
        }
    }
}