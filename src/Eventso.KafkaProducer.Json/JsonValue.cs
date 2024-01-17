using System.Buffers;
using System.Text.Json;

namespace Eventso.KafkaProducer.Json;

public readonly struct JsonValue<T>(
    T value,
    JsonWriterOptions writerOptions = default,
    JsonSerializerOptions? serializerOptions = default) : IBinaryBufferWritable
{
    public void WriteBytes(IBufferWriter<byte> buffer)
        => JsonSerializer.Serialize(new Utf8JsonWriter(buffer, writerOptions), value, serializerOptions);
}