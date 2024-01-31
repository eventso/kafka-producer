using Confluent.Kafka;

namespace Eventso.KafkaProducer;

/// <summary>
///     Represents an error that occurred whilst producing a message.
/// </summary>
public class ProduceException : KafkaException
{
    /// <summary>
    ///     Initialize a new instance of ProduceException based on 
    ///     an existing Error value.
    /// </summary>
    /// <param name="error"> 
    ///     The error associated with the delivery report.
    /// </param>
    /// <param name="topicPartitionOffset">
    ///     The delivery result associated with the produce request.
    /// </param>
    /// <param name="status">
    /// Persistence status
    /// </param>
    public ProduceException(Error error, TopicPartitionOffset topicPartitionOffset, PersistenceStatus status = default)
        : base(error)
    {
        TopicPartitionOffset = topicPartitionOffset;
        Status = status;
    }

    /// <summary>
    ///     The delivery result associated with the produce request.
    /// </summary>
    public TopicPartitionOffset TopicPartitionOffset { get; }

    public PersistenceStatus Status { get; }
}