using System.Buffers;
using System.Text;
using Confluent.Kafka;
using SpanJson;
using Timestamp = Confluent.Kafka.Timestamp;

namespace Eventso.KafkaProducer;

public static class SpanJsonValueExtensions
{
    public static Task<DeliveryResult> ProduceJsonAsync<T>(
        this IProducer producer,
        string topic,
        ReadOnlySpan<byte> key,
        T value,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        using var valueBytes = new Owner(JsonSerializer.Generic.Utf8.SerializeToArrayPool(value));

        return producer.ProduceAsync(topic, key, valueBytes.value, cancellationToken, headers, timestamp, partition);
    }

    public static void ProduceJson<T>(
        this IProducer producer,
        string topic,
        ReadOnlySpan<byte> key,
        T value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
    {
        using var valueBytes = new Owner(JsonSerializer.Generic.Utf8.SerializeToArrayPool(value));

        producer.Produce(topic, key, valueBytes.value, headers, timestamp, deliveryHandler, partition);
    }

    public static void ProduceJson<T>(
        this MessageBatch batch,
        ReadOnlySpan<byte> key,
        T value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        using var valueBytes = new Owner(JsonSerializer.Generic.Utf8.SerializeToArrayPool(value));
        batch.Produce(key, valueBytes.value, headers, timestamp, partition);
    }

    public static Task<DeliveryResult> ProduceJsonAsync<T>(
        this IProducer producer,
        string topic,
        short key,
        T value,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        using var valueBytes = new Owner(JsonSerializer.Generic.Utf8.SerializeToArrayPool(value));

        return producer.ProduceAsync(topic, key, valueBytes.value, cancellationToken, headers, timestamp, partition);
    }

    public static void ProduceJson<T>(
        this IProducer producer,
        string topic,
        short key,
        T value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
    {
        using var valueBytes = new Owner(JsonSerializer.Generic.Utf8.SerializeToArrayPool(value));

        producer.Produce(topic, key, valueBytes.value, headers, timestamp, deliveryHandler, partition);
    }

    public static void ProduceJson<T>(
        this MessageBatch batch,
        short key,
        T value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        using var valueBytes = new Owner(JsonSerializer.Generic.Utf8.SerializeToArrayPool(value));
        batch.Produce(key, valueBytes.value, headers, timestamp, partition);
    }

    public static Task<DeliveryResult> ProduceJsonAsync<T>(
        this IProducer producer,
        string topic,
        int key,
        T value,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        using var valueBytes = new Owner(JsonSerializer.Generic.Utf8.SerializeToArrayPool(value));

        return producer.ProduceAsync(topic, key, valueBytes.value, cancellationToken, headers, timestamp, partition);
    }

    public static void ProduceJson<T>(
        this IProducer producer,
        string topic,
        int key,
        T value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
    {
        using var valueBytes = new Owner(JsonSerializer.Generic.Utf8.SerializeToArrayPool(value));

        producer.Produce(topic, key, valueBytes.value, headers, timestamp, deliveryHandler, partition);
    }

    public static void ProduceJson<T>(
        this MessageBatch batch,
        int key,
        T value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        using var valueBytes = new Owner(JsonSerializer.Generic.Utf8.SerializeToArrayPool(value));
        batch.Produce(key, valueBytes.value, headers, timestamp, partition);
    }

    public static Task<DeliveryResult> ProduceJsonAsync<T>(
        this IProducer producer,
        string topic,
        long key,
        T value,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        using var valueBytes = new Owner(JsonSerializer.Generic.Utf8.SerializeToArrayPool(value));

        return producer.ProduceAsync(topic, key, valueBytes.value, cancellationToken, headers, timestamp, partition);
    }

    public static void ProduceJson<T>(
        this IProducer producer,
        string topic,
        long key,
        T value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
    {
        using var valueBytes = new Owner(JsonSerializer.Generic.Utf8.SerializeToArrayPool(value));

        producer.Produce(topic, key, valueBytes.value, headers, timestamp, deliveryHandler, partition);
    }

    public static void ProduceJson<T>(
        this MessageBatch batch,
        long key,
        T value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        using var valueBytes = new Owner(JsonSerializer.Generic.Utf8.SerializeToArrayPool(value));
        batch.Produce(key, valueBytes.value, headers, timestamp, partition);
    }

    public static Task<DeliveryResult> ProduceJsonAsync<T>(
        this IProducer producer,
        string topic,
        string? key,
        T value,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null,
        Encoding? keyEncoding = default)
    {
        using var valueBytes = new Owner(JsonSerializer.Generic.Utf8.SerializeToArrayPool(value));

        return producer.ProduceAsync(
            topic,
            key,
            valueBytes.value,
            cancellationToken,
            headers,
            timestamp,
            partition,
            keyEncoding);
    }

    public static void ProduceJson<T>(
        this IProducer producer,
        string topic,
        string? key,
        T value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null,
        Encoding? keyEncoding = default)
    {
        using var valueBytes = new Owner(JsonSerializer.Generic.Utf8.SerializeToArrayPool(value));

        producer.Produce(
            topic,
            key,
            valueBytes.value,
            headers,
            timestamp,
            deliveryHandler,
            partition,
            keyEncoding);
    }

    public static void ProduceJson<T>(
        this MessageBatch batch,
        string? key,
        T value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null,
        Encoding? keyEncoding = default)
    {
        using var valueBytes = new Owner(JsonSerializer.Generic.Utf8.SerializeToArrayPool(value));
        batch.Produce(
            key,
            valueBytes.value,
            headers,
            timestamp,
            partition,
            keyEncoding);
    }


    /// Converts Guid to bytes with big endian bytes ordering  
    public static Task<DeliveryResult> ProduceJsonAsync<T>(
        this IProducer producer,
        string topic,
        Guid key,
        T value,
        CancellationToken cancellationToken = default,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        using var valueBytes = new Owner(JsonSerializer.Generic.Utf8.SerializeToArrayPool(value));

        return producer.ProduceAsync(topic, key, valueBytes.value, cancellationToken, headers, timestamp, partition);
    }


    /// Converts Guid to bytes with big endian bytes ordering  
    public static void ProduceJson<T>(
        this IProducer producer,
        string topic,
        Guid key,
        T value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
    {
        using var valueBytes = new Owner(JsonSerializer.Generic.Utf8.SerializeToArrayPool(value));

        producer.Produce(topic, key, valueBytes.value, headers, timestamp, deliveryHandler, partition);
    }

    /// Converts Guid to bytes with big endian bytes ordering  
    public static void ProduceJson<T>(
        this MessageBatch batch,
        Guid key,
        T value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        using var valueBytes = new Owner(JsonSerializer.Generic.Utf8.SerializeToArrayPool(value));
        batch.Produce(key, valueBytes.value, headers, timestamp, partition);
    }

    private readonly ref struct Owner
    {
        public readonly ArraySegment<byte> value;

        public Owner(ArraySegment<byte> value)
            => this.value = value;

        public void Dispose()
            => ArrayPool<byte>.Shared.Return(value.Array!);
    }
}