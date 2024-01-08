using Confluent.Kafka;

namespace Eventso.KafkaProducer;

internal sealed class ActionDeliveryHandler : IDeliveryHandler
{
    private readonly string topic;
    private readonly Action<DeliveryReport>? handler;

    public ActionDeliveryHandler(string topic, Action<DeliveryReport>? handler)
    {
        this.topic = topic;
        this.handler = handler;
    }

    public void HandleDeliveryReport(DeliveryReport<Null, Null>? deliveryReport)
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
            Headers = deliveryReport.Message.Headers,
            Topic = topic
        };

        if (handler != null)
        {
            handler(dr);
        }
    }
}