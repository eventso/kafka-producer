using System.Buffers;

namespace Eventso.KafkaProducer;

public interface IBinaryBufferWritable
{
    void WriteBytes(IBufferWriter<byte> buffer);
}