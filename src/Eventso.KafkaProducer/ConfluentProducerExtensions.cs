using Eventso.KafkaProducer;

// ReSharper disable once CheckNamespace
namespace Confluent.Kafka;

public static class ConfluentProducerExtensions
{
    public static IProducer CreateBinary<_, __>(this IProducer<_, __> producer)
    {
        var dependentBuilder = new DependentProducerBuilder<byte[], byte[]>(producer.Handle);

        return new Producer(dependentBuilder.Build());
    }

    public static Task<DeliveryResult> ProduceAsync<_, __>(
        this IProducer<_, __> producer,
        string topic,
        ReadOnlySpan<byte> key,
        ReadOnlySpan<byte> value,
        CancellationToken cancellationToken = default(CancellationToken),
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        if (cancellationToken.IsCancellationRequested)
            return Task.FromCanceled<DeliveryResult>(cancellationToken);

        var handler = new TaskDeliveryHandler(topic, cancellationToken);

        try
        {
            BinaryProducer.Produce(
                producer.Handle.LibrdkafkaHandle,
                topic,
                value,
                key,
                timestamp,
                partition ?? Partition.Any,
                headers?.BackingList,
                handler);

            return handler.Task;
        }
        catch (KafkaException ex)
        {
            handler.Release();

            throw new ProduceException(
                ex.Error,
                new(topic, partition ?? Partition.Any, Offset.Unset));
        }
        catch
        {
            handler.Release();
            throw;
        }
    }

    public static void Produce<_, __>(
        this IProducer<_, __> producer,
        string topic,
        ReadOnlySpan<byte> key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
    {
        try
        {
            BinaryProducer.Produce(
                producer.Handle.LibrdkafkaHandle,
                topic,
                value,
                key,
                timestamp,
                partition ?? Partition.Any,
                headers?.BackingList,
                deliveryHandler == null
                    ? null
                    : new ActionDeliveryHandler(topic, deliveryHandler));
        }
        catch (KafkaException ex)
        {
            throw new ProduceException(
                ex.Error,
                new(topic, partition ?? Partition.Any, Offset.Unset));
        }
    }
}