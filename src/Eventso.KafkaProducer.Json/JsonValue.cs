using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Eventso.KafkaProducer;

public readonly struct JsonValue<T>(
    T value,
    JsonSerializerOptions? serializerOptions = default,
    JsonWriterOptions writerOptions = default) : IBinaryBufferWritable
{
    public void WriteBytes(IBufferWriter<byte> buffer)
        => JsonSerializer.Serialize(
            new Utf8JsonWriter(buffer, writerOptions),
            value,
            serializerOptions);
}

public readonly struct JsonContextValue<T>(
    T value,
    JsonSerializerContext serializerContext,
    JsonWriterOptions writerOptions = default) : IBinaryBufferWritable
{
    public void WriteBytes(IBufferWriter<byte> buffer)
        => JsonSerializer.Serialize(
            new Utf8JsonWriter(buffer, writerOptions),
            value,
            typeof(T),
            serializerContext);
}