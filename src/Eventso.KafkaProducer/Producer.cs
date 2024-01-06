using Confluent.Kafka;

namespace Eventso.KafkaProducer;

/// <summary>
/// Binary producer.
/// </summary>
public sealed class Producer : IProducer
{
    private readonly IProducer<byte[], byte[]> producerImplementation;

    public Producer(IProducer<byte[], byte[]> inner)
    {
        producerImplementation = inner;
    }

    public Task<DeliveryResult> ProduceAsync(
        TopicPartition topicPartition,
        ReadOnlySpan<byte> key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var handler = new TypedTaskDeliveryHandlerShim(topicPartition.Topic);

            if (cancellationToken.CanBeCanceled)
                handler.CancellationTokenRegistration = cancellationToken.Register(() => handler.TrySetCanceled());

            BinaryProducer.Produce(
                producerImplementation.Handle.LibrdkafkaHandle,
                topicPartition.Topic,
                value,
                key,
                timestamp,
                topicPartition.Partition,
                headers?.BackingList,
                handler);

            return handler.Task;
        }
        catch (KafkaException ex)
        {
            throw new ProduceException(
                ex.Error,
                new DeliveryResult
                {
                    TopicPartitionOffset = new TopicPartitionOffset(topicPartition, Offset.Unset)
                });
        }
    }

    /// <inheritdoc/>
    public Task<DeliveryResult> ProduceAsync(
        string topic,
        ReadOnlySpan<byte> key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        CancellationToken cancellationToken = default)
        => ProduceAsync(
            new TopicPartition(topic, Partition.Any),
            key,
            value,
            headers,
            timestamp,
            cancellationToken);

    /// <inheritdoc/>
    public void Produce(
        string topic,
        ReadOnlySpan<byte> key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null)
        => Produce(
            new TopicPartition(topic, Partition.Any),
            key,
            value,
            headers,
            timestamp,
            deliveryHandler);


    /// <inheritdoc/>
    public void Produce(
        TopicPartition topicPartition,
        ReadOnlySpan<byte> key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null)
    {
        try
        {
            BinaryProducer.Produce(
                producerImplementation.Handle.LibrdkafkaHandle,
                topicPartition.Topic,
                value,
                key,
                timestamp, topicPartition.Partition,
                headers?.BackingList,
                deliveryHandler == null
                    ? null
                    : new TypedDeliveryHandlerShim_Action(
                        topicPartition.Topic,
                        deliveryHandler));
        }
        catch (KafkaException ex)
        {
            throw new ProduceException(
                ex.Error,
                new DeliveryReport
                {
                    TopicPartitionOffset = new TopicPartitionOffset(topicPartition, Offset.Unset)
                });
        }
    }


    private class TypedTaskDeliveryHandlerShim : TaskCompletionSource<DeliveryResult>, IDeliveryHandler
    {
        public TypedTaskDeliveryHandlerShim(string topic)
            : base(TaskCreationOptions.RunContinuationsAsynchronously)
        {
            Topic = topic;
        }

        public CancellationTokenRegistration CancellationTokenRegistration;

        public string Topic;

        public void HandleDeliveryReport(DeliveryReport<Null, Null>? deliveryReport)
        {
            CancellationTokenRegistration.Dispose();

            if (deliveryReport == null)
            {
                TrySetResult(new DeliveryResult());
                return;
            }

            var dr = new DeliveryResult
            {
                TopicPartitionOffset = deliveryReport.TopicPartitionOffset,
                Status = deliveryReport.Status,
                Timestamp = deliveryReport.Message.Timestamp,
                Headers = deliveryReport.Message.Headers
            };
            // topic is cached in this object, not set in the deliveryReport to avoid the 
            // cost of marshalling it.
            dr.Topic = Topic;

            if (deliveryReport.Error.IsError)
            {
                TrySetException(new ProduceException(deliveryReport.Error, dr));
            }
            else
            {
                TrySetResult(dr);
            }
        }
    }

    private class TypedDeliveryHandlerShim_Action : IDeliveryHandler
    {
        public TypedDeliveryHandlerShim_Action(string topic, Action<DeliveryReport> handler)
        {
            Topic = topic;
            Handler = handler;
        }

        public string Topic;

        public Action<DeliveryReport> Handler;

        public void HandleDeliveryReport(DeliveryReport<Null, Null> deliveryReport)
        {
            if (deliveryReport == null)
            {
                return;
            }

            var dr = new DeliveryReport
            {
                TopicPartitionOffsetError = deliveryReport.TopicPartitionOffsetError,
                Status = deliveryReport.Status,
                Timestamp = deliveryReport.Message.Timestamp,
                Headers = deliveryReport.Message.Headers
            };
            // topic is cached in this object, not set in the deliveryReport to avoid the 
            // cost of marshalling it.
            dr.Topic = Topic;

            if (Handler != null)
            {
                Handler(dr);
            }
        }
    }

    public int Poll(TimeSpan timeout)
        => producerImplementation.Poll(timeout);

    public int Flush(TimeSpan timeout)
        => producerImplementation.Flush(timeout);

    public void Flush(CancellationToken cancellationToken = default(CancellationToken))
        => producerImplementation.Flush(cancellationToken);

    public void Dispose()
        => producerImplementation.Dispose();

    public Handle Handle => producerImplementation.Handle;

    public string Name => producerImplementation.Name;

    public int AddBrokers(string brokers)
        => producerImplementation.AddBrokers(brokers);

    public void SetSaslCredentials(string username, string password)
        => producerImplementation.SetSaslCredentials(username, password);

    public void InitTransactions(TimeSpan timeout)
        => producerImplementation.InitTransactions(timeout);

    public void BeginTransaction()
        => producerImplementation.BeginTransaction();

    public void CommitTransaction(TimeSpan timeout)
        => producerImplementation.CommitTransaction(timeout);

    public void CommitTransaction()
        => producerImplementation.CommitTransaction();

    public void AbortTransaction(TimeSpan timeout)
        => producerImplementation.AbortTransaction(timeout);

    public void AbortTransaction()
        => producerImplementation.AbortTransaction();

    public void SendOffsetsToTransaction(
        IEnumerable<TopicPartitionOffset> offsets,
        IConsumerGroupMetadata groupMetadata,
        TimeSpan timeout)
        => producerImplementation.SendOffsetsToTransaction(offsets, groupMetadata, timeout);
}