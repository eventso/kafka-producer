using System.Text.Json.Serialization;

namespace Eventso.KafkaProducer.Benchmark;

[JsonSourceGenerationOptions(GenerationMode = JsonSourceGenerationMode.Serialization)]
[JsonSerializable(typeof(Producing.Order))]
internal partial class SourceGenerationContext : JsonSerializerContext
{
}