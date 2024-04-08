using System.Buffers;
using Google.Protobuf;

namespace Eventso.KafkaProducer;

public readonly struct ProtoSpanValue : IBinarySerializable
{
    private readonly IMessage value;
    private readonly int size;

    public ProtoSpanValue(IMessage value)
    {
        this.value = value;
        size = value.CalculateSize();
    }

    public int GetSize() => size;

    public int WriteBytes(Span<byte> destination)
    {
        value.WriteTo(destination[..size]);

        return size;
    }
}

public readonly struct ProtoBufferValue(IMessage value) : IBinaryBufferWritable
{
    public void WriteBytes(IBufferWriter<byte> buffer)
        => value.WriteTo(buffer);
}