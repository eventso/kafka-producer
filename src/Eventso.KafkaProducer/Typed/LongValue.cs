using System.Buffers.Binary;

namespace Eventso.KafkaProducer;

public readonly struct LongValue(long value) : IBinarySerializable
{
    public int Size => sizeof(long);

    public int WriteBytes(Span<byte> destination)
    {
        BinaryPrimitives.WriteInt64BigEndian(destination, value);
        return Size;
    }

    public static implicit operator LongValue(long value) => new(value);
}