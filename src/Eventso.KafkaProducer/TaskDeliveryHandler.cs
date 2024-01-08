using Confluent.Kafka;

namespace Eventso.KafkaProducer;

internal sealed class TaskDeliveryHandler : TaskCompletionSource<DeliveryResult>, IDeliveryHandler
{
    private readonly string topic;
    public CancellationTokenRegistration CancellationTokenRegistration;

    public TaskDeliveryHandler(string topic)
        : base(TaskCreationOptions.RunContinuationsAsynchronously)
    {
        this.topic = topic;
    }

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
            Headers = deliveryReport.Message.Headers,
            Topic = topic
        };

        if (deliveryReport.Error.IsError)
            TrySetException(new ProduceException(deliveryReport.Error, dr));
        else
            TrySetResult(dr);
    }
}