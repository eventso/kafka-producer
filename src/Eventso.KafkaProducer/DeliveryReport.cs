using Confluent.Kafka;

namespace Eventso.KafkaProducer;

/// <summary>
///     The result of a produce request.
/// </summary>
public class DeliveryReport : DeliveryResult
{
    public DeliveryReport(DeliveryReport<Null, Null> inner, string topic)
        : base(inner, topic)
    {
        Error = inner.Error;
    }

    /// <summary>
    ///     An error (or NoError) associated with the message.
    /// </summary>
    public Error? Error { get; }

    /// <summary>
    ///     The TopicPartitionOffsetError associated with the message.
    /// </summary>
    public TopicPartitionOffsetError TopicPartitionOffsetError => new(Topic, Partition, Offset, Error);
}