using Confluent.Kafka;

namespace Eventso.KafkaProducer;

internal sealed class TaskDeliveryHandler : TaskCompletionSource<DeliveryResult>, IDeliveryHandler, IDisposable
{
    private readonly string topic;
    private readonly CancellationTokenRegistration cancellationTokenRegistration;

    public TaskDeliveryHandler(string topic, CancellationToken token)
        : base(TaskCreationOptions.RunContinuationsAsynchronously)
    {
        this.topic = topic;

        if (token.CanBeCanceled)
            cancellationTokenRegistration = token.UnsafeRegister(arg => ((TaskDeliveryHandler)arg!).TrySetCanceled(), this);
    }

    public void HandleDeliveryReport(DeliveryReport<Null, Null>? deliveryReport)
    {
        cancellationTokenRegistration.Dispose();

        if (deliveryReport == null)
        {
            TrySetResult(new DeliveryResult(new DeliveryResult<Null, Null>(), topic));
            return;
        }

        if (deliveryReport.Error.IsError)
            TrySetException(new ProduceException(
                deliveryReport.Error,
                new(topic, deliveryReport.Partition, deliveryReport.Offset),
                deliveryReport.Status));
        else
            TrySetResult(new DeliveryResult(deliveryReport, topic));
    }

    public void Dispose()
        => cancellationTokenRegistration.Dispose();
}