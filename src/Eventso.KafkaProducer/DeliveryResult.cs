using Confluent.Kafka;

namespace Eventso.KafkaProducer;

/// <summary>
///     Encapsulates the result of a successful produce request.
/// </summary>
public class DeliveryResult(DeliveryResult<Null, Null> inner, string topic)
{
    /// <summary>
    ///     The topic associated with the message.
    /// </summary>
    public string Topic => topic;

    /// <summary>
    ///     The partition associated with the message.
    /// </summary>
    public Partition Partition => inner.Partition;

    /// <summary>
    ///     The partition offset associated with the message.
    /// </summary>
    public Offset Offset => inner.Offset;

    /// <summary>
    ///     The TopicPartition associated with the message.
    /// </summary>
    public TopicPartition TopicPartition => new TopicPartition(Topic, Partition);

    /// <summary>
    ///     The TopicPartitionOffset associated with the message.
    /// </summary>
    public TopicPartitionOffset TopicPartitionOffset
    {
        get => new(Topic, Partition, Offset);
    }

    /// <summary>
    ///     The persistence status of the message
    /// </summary>
    public PersistenceStatus Status => inner.Status;

    /// <summary>
    ///     The Kafka message timestamp.
    /// </summary>
    public Timestamp Timestamp => inner.Timestamp;

    /// <summary>
    ///     The Kafka message headers.
    /// </summary>
    public Headers? Headers => inner.Headers;
}