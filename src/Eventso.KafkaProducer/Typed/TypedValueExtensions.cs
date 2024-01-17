using System.Buffers;
using CommunityToolkit.HighPerformance.Buffers;
using Confluent.Kafka;

namespace Eventso.KafkaProducer;

public static class TypedValueExtensions
{
    private const int StackThreshold = 256;

    public static Task<DeliveryResult> ProduceAsync<TValue>(
        this IProducer producer,
        string topic,
        ReadOnlySpan<byte> key,
        in TValue value,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        where TValue : IBinarySerializable
    {
        var valueStackThreshold = StackThreshold;
        var valueSize = value.GetSize();
        var valueBytesPooled = valueSize <= valueStackThreshold ? null : ArrayPool<byte>.Shared.Rent(valueSize);
        var valueBytes = valueSize == 0 ? Span<byte>.Empty : valueBytesPooled ?? stackalloc byte[valueSize];

        try
        {
            var valueBytesWritten = value.WriteBytes(valueBytes);

            return producer.ProduceAsync(
                topic,
                key,
                valueBytes[..valueBytesWritten],
                cancellationToken,
                headers,
                timestamp,
                partition);
        }
        finally
        {
            if (valueBytesPooled != null)
                ArrayPool<byte>.Shared.Return(valueBytesPooled);
        }
    }

    public static void Produce<TValue>(
        this IProducer producer,
        string topic,
        ReadOnlySpan<byte> key,
        in TValue value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
        where TValue : IBinarySerializable
    {
        var valueStackThreshold = StackThreshold;
        var valueSize = value.GetSize();
        var valueBytesPooled = valueSize <= valueStackThreshold ? null : ArrayPool<byte>.Shared.Rent(valueSize);
        var valueBytes = valueSize == 0 ? Span<byte>.Empty : valueBytesPooled ?? stackalloc byte[valueSize];

        try
        {
            var valueBytesWritten = value.WriteBytes(valueBytes);

            producer.Produce(
                topic,
                key,
                valueBytes[..valueBytesWritten],
                headers,
                timestamp,
                deliveryHandler,
                partition);
        }
        finally
        {
            if (valueBytesPooled != null)
                ArrayPool<byte>.Shared.Return(valueBytesPooled);
        }
    }

    public static void Produce<TValue>(
        this MessageBatch batch,
        ReadOnlySpan<byte> key,
        in TValue value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        where TValue : IBinarySerializable
    {
        var valueStackThreshold = StackThreshold;
        var valueSize = value.GetSize();
        var valueBytesPooled = valueSize <= valueStackThreshold ? null : ArrayPool<byte>.Shared.Rent(valueSize);
        var valueBytes = valueSize == 0 ? Span<byte>.Empty : valueBytesPooled ?? stackalloc byte[valueSize];

        try
        {
            var valueBytesWritten = value.WriteBytes(valueBytes);

            batch.Produce(
                key,
                valueBytes[..valueBytesWritten],
                headers,
                timestamp,
                partition);
        }
        finally
        {
            if (valueBytesPooled != null)
                ArrayPool<byte>.Shared.Return(valueBytesPooled);
        }
    }

    public static Task<DeliveryResult> ProduceAsync<TValue>(
        this IProducer producer,
        string topic,
        ReadOnlySpan<byte> key,
        in TValue value,
        IBuffer<byte> buffer,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        where TValue : IBinaryBufferWritable
    {
        buffer.Clear();
        value.WriteBytes(buffer);

        return producer.ProduceAsync(
            topic,
            key,
            buffer.WrittenSpan,
            cancellationToken,
            headers,
            timestamp,
            partition);
    }

    public static void Produce<TValue>(
        this IProducer producer,
        string topic,
        ReadOnlySpan<byte> key,
        in TValue value,
        IBuffer<byte> buffer,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
        where TValue : IBinaryBufferWritable
    {
        buffer.Clear();
        value.WriteBytes(buffer);

        producer.Produce(
            topic,
            key,
            buffer.WrittenSpan,
            headers,
            timestamp,
            deliveryHandler,
            partition);
    }

    public static void Produce<TValue>(
        this MessageBatch batch,
        ReadOnlySpan<byte> key,
        in TValue value,
        IBuffer<byte> buffer,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        where TValue : IBinaryBufferWritable
    {
        buffer.Clear();
        value.WriteBytes(buffer);

        batch.Produce(
            key,
            buffer.WrittenSpan,
            headers,
            timestamp,
            partition);
    }
}