using Confluent.Kafka;

namespace Eventso.KafkaProducer;

/// <summary>
/// Binary producer.
/// </summary>
public sealed class Producer : IProducer
{
    private readonly IProducer<byte[], byte[]> inner;

    public Producer(IProducer<byte[], byte[]> inner)
    {
        this.inner = inner;
    }

    /// <inheritdoc/>
    public Task<DeliveryResult> ProduceAsync(
        string topic,
        ReadOnlySpan<byte> key,
        ReadOnlySpan<byte> value,
        CancellationToken cancellationToken = default(CancellationToken),
        Headers? headers = null,
        Timestamp timestamp = default,
        Partition? partition = null)
    {
        return inner.ProduceAsync(
            topic,
            key,
            value,
            cancellationToken,
            headers,
            timestamp,
            partition);
    }

    /// <inheritdoc/>
    public void Produce(
        string topic,
        ReadOnlySpan<byte> key,
        ReadOnlySpan<byte> value,
        Headers? headers = null,
        Timestamp timestamp = default,
        Action<DeliveryReport>? deliveryHandler = null,
        Partition? partition = null)
    {
        inner.Produce(
            topic,
            key,
            value,
            headers,
            timestamp,
            deliveryHandler,
            partition);
    }

    public int Poll(TimeSpan timeout)
        => inner.Poll(timeout);

    public int Flush(TimeSpan timeout)
        => inner.Flush(timeout);

    public void Flush(CancellationToken cancellationToken = default(CancellationToken))
        => inner.Flush(cancellationToken);

    public void Dispose()
        => inner.Dispose();

    public Handle Handle => inner.Handle;

    public string Name => inner.Name;

    public int AddBrokers(string brokers)
        => inner.AddBrokers(brokers);

    public void SetSaslCredentials(string username, string password)
        => inner.SetSaslCredentials(username, password);

    public void InitTransactions(TimeSpan timeout)
        => inner.InitTransactions(timeout);

    public void BeginTransaction()
        => inner.BeginTransaction();

    public void CommitTransaction(TimeSpan timeout)
        => inner.CommitTransaction(timeout);

    public void CommitTransaction()
        => inner.CommitTransaction();

    public void AbortTransaction(TimeSpan timeout)
        => inner.AbortTransaction(timeout);

    public void AbortTransaction()
        => inner.AbortTransaction();

    public void SendOffsetsToTransaction(
        IEnumerable<TopicPartitionOffset> offsets,
        IConsumerGroupMetadata groupMetadata,
        TimeSpan timeout)
        => inner.SendOffsetsToTransaction(offsets, groupMetadata, timeout);
}