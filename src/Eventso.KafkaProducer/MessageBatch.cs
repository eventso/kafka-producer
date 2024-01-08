using Confluent.Kafka;

namespace Eventso.KafkaProducer;

/// <summary>
/// Produce message batch.
/// </summary>
public sealed class MessageBatch
{
    private readonly IProducer producer;
    private readonly string topic;
    private readonly DeliveryCounterHandler handler;
    private int sentCount;
    private bool completed;

    public MessageBatch(IProducer producer, string topic)
    {
        this.producer = producer;
        this.topic = topic;
        handler = new DeliveryCounterHandler(topic);
    }

    /// <summary>
    ///     Asynchronously send a single message to a
    ///     Kafka topic. The partition the message is
    ///     sent to is determined by the partitioner
    ///     defined using the 'partitioner' configuration
    ///     property or partition parameter when specified.
    ///     Method is thread safe.
    /// </summary>
    /// <param name="key">
    ///     The message key. 'ReadOnlySpan&lt;byte&gt;.Empty' can be used for null key.
    /// </param>
    /// <param name="value">
    ///     The message value. 'ReadOnlySpan&lt;byte&gt;.Empty' can be used for null value.
    /// </param>
    /// <param name="headers">
    ///     The collection of message headers (or null). Specifying null or an 
    ///     empty list are equivalent. The order of headers is maintained, and
    ///     duplicate header keys are allowed.
    /// </param>
    /// <param name="timestamp">
    ///     The message timestamp. The timestamp type must be set to CreateTime. 
    ///     Specify Timestamp.Default to set the message timestamp to the time
    ///     of this function call.
    /// </param>
    /// <param name="partition">The partition or null for using partitioner</param>
    /// <exception cref="ProduceException">
    ///     Thrown in response to any produce request
    ///     that was unsuccessful for any reason
    ///     (excluding user application logic errors).
    ///     The Error property of the exception provides
    ///     more detailed information.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    ///     Thrown when batch was completed.
    /// </exception>
    public void Produce(
        ReadOnlySpan<byte> key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        if (Volatile.Read(ref completed) || handler.Task.IsCompleted)
            throw new InvalidOperationException("Batch is already completed.");

        if (handler.Task.IsFaulted)
            throw new InvalidOperationException("Producing faulted.", handler.Task.Exception);

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

            Interlocked.Increment(ref sentCount);
        }
        catch (KafkaException ex)
        {
            throw new ProduceException(
                ex.Error,
                new DeliveryResult
                {
                    TopicPartitionOffset = new TopicPartitionOffset(new(topic, partition ?? Partition.Any), Offset.Unset)
                });
        }
    }

    /// <summary>
    ///     Complete messages producing. 
    /// </summary>
    /// <returns>
    ///     A Task which will complete when all messages in the batch
    ///     will be delivered.
    /// </returns>
    public Task Complete(CancellationToken token)
    {
        Volatile.Write(ref completed, true);

        if (token.CanBeCanceled)
            handler.CancellationTokenRegistration = token.Register(() => handler.TrySetCanceled());

        handler.Complete(sentCount);

        return handler.Task;
    }

    private sealed class DeliveryCounterHandler : TaskCompletionSource, IDeliveryHandler
    {
        private readonly string topic;
        private int deliveryCount = 0;
        public CancellationTokenRegistration CancellationTokenRegistration;

        public DeliveryCounterHandler(string topic)
            : base(TaskCreationOptions.RunContinuationsAsynchronously)
        {
            this.topic = topic;
        }

        public void HandleDeliveryReport(DeliveryReport<Null, Null>? deliveryReport)
        {
            if (deliveryReport?.Error?.IsError != true)
            {
                var residual = Interlocked.Decrement(ref deliveryCount);

                TryComplete(residual);
            }
            else
            {
                var result = new DeliveryResult
                {
                    TopicPartitionOffset = deliveryReport.TopicPartitionOffset,
                    Status = deliveryReport.Status,
                    Timestamp = deliveryReport.Message.Timestamp,
                    Headers = deliveryReport.Message.Headers,
                    Topic = topic
                };

                TrySetException(new ProduceException(deliveryReport.Error, result));
            }
        }

        public void Complete(int sentCount)
        {
            var residual = Interlocked.Add(ref deliveryCount, sentCount);

            TryComplete(residual);
        }

        private void TryComplete(int residual)
        {
            if (residual != 0)
                return;

            CancellationTokenRegistration.Dispose();
            TrySetResult();
        }
    }
}