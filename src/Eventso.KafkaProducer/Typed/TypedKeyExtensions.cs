﻿using System.Buffers;
using System.Runtime.CompilerServices;
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
        => producer.Produce<ShortValue>(topic, key, value, headers, timestamp, deliveryHandler, partition);

    public static void Produce(
        this MessageBatch batch,
        short key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => batch.Produce<ShortValue>(key, value, headers, timestamp, partition);

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
        => producer.Produce<IntValue>(topic, key, value, headers, timestamp, deliveryHandler, partition);

    public static void Produce(
        this MessageBatch batch,
        int key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => batch.Produce<IntValue>(key, value, headers, timestamp, partition);

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
        => producer.Produce<LongValue>(topic, key, value, headers, timestamp, deliveryHandler, partition);

    public static void Produce(
        this MessageBatch batch,
        long key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => batch.Produce<LongValue>(key, value, headers, timestamp, partition);

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
        return producer.ProduceAsync<StringValue>(
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
        producer.Produce<StringValue>(
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
        => batch.Produce<StringValue>(keyEncoding == null ? new(key) : new(key, keyEncoding), value, headers, timestamp, partition);


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
        => producer.Produce<GuidValue>(topic, key, value, headers, timestamp, deliveryHandler, partition);

    /// Converts Guid to bytes with big endian bytes ordering  
    public static void Produce(
        this MessageBatch batch,
        Guid key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        => batch.Produce<GuidValue>(key, value, headers, timestamp, partition);

    [SkipLocalsInit]
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
        var keySize = key.Size;
        byte[]? keyBytesPooled = null;
        var keyBytes = keySize <= StackThreshold ? stackalloc byte[keySize] : keyBytesPooled = ArrayPool<byte>.Shared.Rent(keySize);

        try
        {
            var bytesWritten = key.WriteBytes(keyBytes);

            return producer.ProduceAsync(topic, keyBytes[..bytesWritten], value, cancellationToken, headers, timestamp, partition);
        }
        finally
        {
            if (keyBytesPooled != null)
                ArrayPool<byte>.Shared.Return(keyBytesPooled);
        }
    }

    [SkipLocalsInit]
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
        var keySize = key.Size;
        byte[]? keyBytesPooled = null;
        var keyBytes = keySize <= StackThreshold ? stackalloc byte[keySize] : keyBytesPooled = ArrayPool<byte>.Shared.Rent(keySize);

        try
        {
            var bytesWritten = key.WriteBytes(keyBytes);

            producer.Produce(topic, keyBytes[..bytesWritten], value, headers, timestamp, deliveryHandler, partition);
        }
        finally
        {
            if (keyBytesPooled != null)
                ArrayPool<byte>.Shared.Return(keyBytesPooled);
        }
    }

    [SkipLocalsInit]
    private static void Produce<TKey>(
        this MessageBatch batch,
        TKey key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
        where TKey : IBinarySerializable
    {
        var keySize = key.Size;
        byte[]? keyBytesPooled = null;
        var keyBytes = keySize <= StackThreshold ? stackalloc byte[keySize] : keyBytesPooled = ArrayPool<byte>.Shared.Rent(keySize);

        try
        {
            var bytesWritten = key.WriteBytes(keyBytes);

            batch.Produce(keyBytes[..bytesWritten], value, headers, timestamp, partition);
        }
        finally
        {
            if (keyBytesPooled != null)
                ArrayPool<byte>.Shared.Return(keyBytesPooled);
        }
    }
}