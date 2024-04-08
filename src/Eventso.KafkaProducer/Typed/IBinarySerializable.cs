namespace Eventso.KafkaProducer;

public interface IBinarySerializable
{
    int Size { get; }
    int WriteBytes(Span<byte> destination);
}