using System.Buffers;
using Google.Protobuf;

namespace Eventso.KafkaProducer;

public readonly struct ProtoBufferValue(IMessage value) : IBinaryBufferWritable
{
    public void WriteBytes(IBufferWriter<byte> buffer)
        => value.WriteTo(buffer);
}