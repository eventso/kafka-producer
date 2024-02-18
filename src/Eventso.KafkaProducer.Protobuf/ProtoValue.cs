using System.Buffers;
using Google.Protobuf;

namespace Eventso.KafkaProducer;

public readonly struct ProtoValue(IMessage value) : IBinarySerializable, IBinaryBufferWritable
{
    public int GetSize() => value.CalculateSize();

    /// The size of the destination span needs to fit the serialized size
    /// of the message exactly, otherwise an exception is thrown.
    public int WriteBytes(Span<byte> destination)
    {
        value.WriteTo(destination);

        return destination.Length;
    }

    public void WriteBytes(IBufferWriter<byte> buffer)
        => value.WriteTo(buffer);
}