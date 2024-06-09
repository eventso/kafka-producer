# Binary low-allocating Kafka producer for .Net #

[![NuGet](https://img.shields.io/nuget/v/Eventso.KafkaProducer.svg)](https://www.nuget.org/packages/Eventso.KafkaProducer/)

Binary Kafka producer with a unified api that accepts `ReadOnlySpan<byte>` instead of `Message<TKey,TValue>` and avoids creating intermediate byte arrays during serialization. Uses low-level api of [Confluent.Kafka](https://github.com/confluentinc/confluent-kafka-dotnet) library and is fully compatible with it. A single producer instance is enough for your entire application.


### Features ###
* Binary producer api accepts `ReadOnlySpan<byte>` for key and value
* Only single producer instance in application is enough
* Effectively producing message batch
* Ready to use overloads for common key types: short, int, long, string, Guid
* Support for Protobuf, System.Text.Json, SpanJson serialization (separate packages)
* Confluent.Kafka compatibility

## Registration
There are 3 ways to create binary producer:
```csharp
// 1. Create from Confluent.Kafka ProducerBuilder<TAnyKey, TAnyValue>

ProducerBuilder<byte[], byte[]> confluentBuilder = ...

IProducer binaryProducer = confluentBuilder.BuildBinary();

// 2. Using non-generic ProducerBuilder with same api as ProducerBuilder<TKey, TValue>

ProducerBuilder producerBuilder = new ProducerBuilder(producerConfig);

producerBuilder
    .SetDefaultPartitioner(...)
    .SetErrorHandler(...);

IProducer binaryProducer = producerBuilder.Build();

// 3. Create binary producer from generic Confluent.Kafka producer

IProducer<TAnyKey, TAnyValue> confluentProducer = ...

IProducer binaryProducer = confluentProducer.CreateBinary();

```

## How to use the producer:
There are two basic methods in IProducer interface 
```csharp
Task<DeliveryResult> ProduceAsync(
    string topic,
    ReadOnlySpan<byte> key,
    ReadOnlySpan<byte> value,
    CancellationToken cancellationToken = default(CancellationToken),
    Headers? headers = null,
    Timestamp timestamp = default,
    Partition? partition = null);

void Produce(
    string topic,
    ReadOnlySpan<byte> key,
    ReadOnlySpan<byte> value,
    Headers? headers = null,
    Timestamp timestamp = default,
    Action<DeliveryReport>? deliveryHandler = null,
    Partition? partition = null);
```

Library contains extension methods for frequently used key types: short, int, long, string, Guid

## MessageBatch
MessageBatch creates only one TaskCompletionSource and Task per batch while original producer api creates TaskCompletionSource per message. Batch Produce supports all producer features.
```csharp
var batch = producer.CreateBatch(topicName);

foreach(var record in records)
{
    batch.Produce(record.Key, record.Value)
}

//wait for delivery all messages
await batch.Complete(token);
```

## Protobuf and Json values
Additional packages contain method overloads that accepts Google.Protobuf.IMessage or typed object as message value. They use stack or ArrayPool for non-allocating serialization.

* [Protobuf](https://www.nuget.org/packages/Eventso.KafkaProducer.Protobuf/) 
* [System.Text.Json](https://www.nuget.org/packages/Eventso.KafkaProducer.Json/) (methods also accepts optional `JsonSerializerOptions` or `JsonSerializerContext`)
* [SpanJson](https://www.nuget.org/packages/Eventso.KafkaProducer.SpanJson/)


## Performance and memory allocation benchmark

[Benchmark source code](https://github.com/eventso/kafka-producer/blob/main/benchmarks/Eventso.KafkaProducer.Benchmark/Producing.cs)


| Method                           | Messages | Mean       | Gen0      | Gen1      | Gen2      | Allocated   |
|--------------------------------- |----------|-----------:|----------:|----------:|----------:|------------:|
| Binary_Proto_Buffer_MessageBatch | 5000     |  29.038 ms |  218.7500 |         - |         - |  1406.83 KB |
| Binary_Proto_WhenAll             | 5000     |  33.454 ms |  400.0000 |  200.0000 |         - |  2481.26 KB |
| Confluent_Proto_WhenAll          | 5000     |  37.963 ms | 1000.0000 |  666.6667 |         - |  6652.58 KB |
| Binary_Proto_Buffer_MessageBatch | 10000    |  52.968 ms |  400.0000 |         - |         - |   2813.2 KB |
| Binary_SpanJson_MessageBatch     | 5000     |  57.299 ms |         - |         - |         - |  1407.25 KB |
| Binary_Json_Buffer_MessageBatch  | 5000     |  59.708 ms |  500.0000 |         - |         - |  3516.85 KB |
| Binary_Proto_WhenAll             | 10000    |  61.909 ms |  666.6667 |  444.4444 |  222.2222 |  4870.61 KB |
| Confluent_Proto_WhenAll          | 10000    |  75.649 ms | 2000.0000 | 1000.0000 |  666.6667 | 13304.45 KB |
| Confluent_Json_WhenAll           | 5000     |  82.425 ms | 1000.0000 |         - |         - |  9583.16 KB |
| Binary_SpanJson_MessageBatch     | 10000    | 107.249 ms |         - |         - |         - |  2814.13 KB |
| Binary_Json_Buffer_MessageBatch  | 10000    | 113.456 ms | 1000.0000 |         - |         - |  7033.18 KB |
| Confluent_Json_WhenAll           | 10000    | 145.569 ms | 3000.0000 | 1000.0000 |         - | 19164.77 KB |
| Binary_Proto_Buffer_MessageBatch | 30000    | 147.694 ms | 1000.0000 |         - |         - |  8438.85 KB |
| Binary_Proto_WhenAll             | 30000    | 173.801 ms | 2000.0000 | 1000.0000 |         - | 14752.95 KB |
| Confluent_Proto_WhenAll          | 30000    | 219.556 ms | 6000.0000 | 2000.0000 | 1000.0000 |  39656.1 KB |
| Binary_Json_Buffer_MessageBatch  | 30000    | 335.746 ms | 3000.0000 |         - |         - | 21096.23 KB |
| Binary_SpanJson_MessageBatch     | 30000    | 339.693 ms | 1000.0000 |         - |         - |   8438.8 KB |
| Confluent_Json_WhenAll           | 30000    | 433.557 ms | 9500.0000 | 3000.0000 | 1500.0000 | 57237.13 KB |



Thanks to [@xeromorph](https://github.com/xeromorph) for the great ideas and help.