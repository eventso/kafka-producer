using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;
using CommunityToolkit.HighPerformance.Buffers;
using Confluent.Kafka;
using Confluent.Kafka.Impl;

namespace Eventso.KafkaProducer;

/// <summary>
/// Produce message batch.
/// </summary>
public sealed class MessageBatch
{
    private readonly SafeKafkaHandle producerHandle;
    private readonly string topic;
    private readonly DeliveryCounterHandler handler;
    private int sentCount;
    private bool completed;
    private ArrayPoolBufferWriter<byte>? bufferWriter;

    private int averageSize = 256;
    private static readonly ConcurrentDictionary<string, int> TopicMessageAverageSize = new();

    public MessageBatch(IProducer producer, string topic)
    {
        this.producerHandle = producer.Handle.LibrdkafkaHandle;
        this.topic = topic;
        handler = new DeliveryCounterHandler(topic);
    }

    /// <summary>
    ///     Asynchronously send a single message to a
    ///     Kafka topic. The partition the message is
    ///     sent to is determined by the partitioner
    ///     defined using the 'partitioner' configuration
    ///     property or partition parameter when specified.
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
        if (completed)
            throw new InvalidOperationException("Batch is already completed.");

        if (bufferWriter != null)
            averageSize = (averageSize + value.Length) / 2;

        try
        {
            BinaryProducer.Produce(
                producerHandle,
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
                new(topic, partition ?? Partition.Any, Offset.Unset));
        }
    }

    /// <summary>
    ///     Complete messages producing. 
    /// </summary>
    /// <returns>
    ///     A Task which will complete when all messages in the batch
    ///     will be delivered.
    /// </returns>
    public Task Complete(CancellationToken token = default)
    {
        completed = true;

        handler.Complete(sentCount);

        if (bufferWriter != null)
        {
            bufferWriter.Dispose();
            TopicMessageAverageSize.AddOrUpdate(topic, averageSize, (_, avg) => (avg + averageSize) / 2);
        }

        return handler.Task.WaitAsync(token);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IBuffer<byte> GetBuffer()
    {
        if (bufferWriter == null)
            InitBuffer();

        return bufferWriter!;
    }

    private void InitBuffer()
    {
        var capacity = TopicMessageAverageSize.TryGetValue(topic, out averageSize)
            ? Math.Max(256, (int)(averageSize * 1.5))
            : 512;

        bufferWriter = new ArrayPoolBufferWriter<byte>(capacity);
    }

    private sealed class DeliveryCounterHandler : TaskCompletionSource, IDeliveryHandler
    {
        private readonly string topic;
        private ConcurrentBag<ProduceException>? exceptions;
        private int deliveryCount = 0;

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
                exceptions ??= new ConcurrentBag<ProduceException>();
                exceptions.Add(new ProduceException(deliveryReport.Error, new(topic, deliveryReport.Partition, deliveryReport.Offset), deliveryReport.Status));
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

            if (exceptions != null)
                TrySetException(new AggregateException(exceptions));
            else
                TrySetResult();
        }
    }
}