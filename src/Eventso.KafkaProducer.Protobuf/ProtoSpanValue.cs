using Google.Protobuf;

namespace Eventso.KafkaProducer;

public readonly struct ProtoSpanValue(IMessage value) : IBinarySerializable
{
    public int Size { get; } = value.CalculateSize();

    public int WriteBytes(Span<byte> destination)
    {
        value.WriteTo(destination[..Size]);

        return Size;
    }
}