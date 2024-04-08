using System.Buffers.Binary;

namespace Eventso.KafkaProducer;

public readonly struct ShortValue(short value) : IBinarySerializable
{
    public int Size => sizeof(short);

    public int WriteBytes(Span<byte> destination)
    {
        BinaryPrimitives.WriteInt16BigEndian(destination, value);
        return Size;
    }

    public static implicit operator ShortValue(short value) => new(value);
}