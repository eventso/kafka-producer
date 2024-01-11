namespace Eventso.KafkaProducer;

public interface IBinarySerializable
{
    int GetSize();
    int WriteBytes(Span<byte> destination);
}