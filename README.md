# Binary low-allocating Kafka producer for .net #

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
Two basic methods in IProducer 
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
MessageBatch creates only one TaskCompletionSource and Task per batch while original producer api creates TaskCompletionSource per messages. Batch Produce supports all producer features.
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
Additional packages contain method oversloads that accepts Google.Protobuf.IMessage or typed object as message value. They use stack or ArrayPool for non-allocating serialization.

* [Protobuf](https://www.nuget.org/packages/Eventso.KafkaProducer.Protobuf/) 
* [System.Text.Json](https://www.nuget.org/packages/Eventso.KafkaProducer.Json/) (methods also accepts optional `JsonSerializerOptions` or `JsonSerializerContext`)
* [SpanJson](https://www.nuget.org/packages/Eventso.KafkaProducer.SpanJson/)


## Performance and memory allocation benchmark (.net 8)

[Benchmark source code](https://github.com/eventso/kafka-producer/blob/main/benchmarks/Eventso.KafkaProducer.Benchmark/Producing.cs)


| Method                           | MessageCount | Mean       | Error      | StdDev     | Median     | Gen0      | Gen1      | Gen2      | Allocated   |
|--------------------------------- |------------- |-----------:|-----------:|-----------:|-----------:|----------:|----------:|----------:|------------:|
| Binary_Proto_Buffer_MessageBatch | 1000         |   9.353 ms |  0.2653 ms |  0.7306 ms |   9.297 ms |   31.2500 |         - |         - |   281.76 KB |
| Binary_Proto_MessageBatch        | 1000         |   9.722 ms |  0.3157 ms |  0.8851 ms |   9.669 ms |   31.2500 |         - |         - |   281.76 KB |
| Binary_Proto_WhenAll             | 1000         |  11.274 ms |  0.3988 ms |  1.1312 ms |  11.157 ms |   62.5000 |         - |         - |   493.02 KB |
| Binary_Proto_Buffer_WhenAll      | 1000         |  11.880 ms |  0.4948 ms |  1.3957 ms |  11.877 ms |   71.4286 |         - |         - |   493.35 KB |
| Confluent_Proto_WhenAll          | 1000         |  12.996 ms |  0.3991 ms |  1.1643 ms |  12.752 ms |  187.5000 |   93.7500 |         - |  1321.33 KB |
| Binary_SpanJson_MessageBatch     | 1000         |  15.136 ms |  0.3758 ms |  1.0413 ms |  15.113 ms |   31.2500 |         - |         - |   281.66 KB |
| Binary_Json_Buffer_MessageBatch  | 1000         |  17.775 ms |  0.7340 ms |  2.1061 ms |  17.934 ms |   93.7500 |         - |         - |   703.67 KB |
| Confluent_Json_WhenAll           | 1000         |  20.693 ms |  0.6509 ms |  1.8884 ms |  20.763 ms |  281.2500 |  156.2500 |         - |  1907.29 KB |
| Binary_Proto_Buffer_MessageBatch | 5000         |  29.038 ms |  0.5780 ms |  1.5726 ms |  28.955 ms |  218.7500 |         - |         - |  1406.83 KB |
| Binary_Proto_MessageBatch        | 5000         |  29.490 ms |  0.7871 ms |  2.2329 ms |  29.511 ms |  200.0000 |         - |         - |  1407.03 KB |
| Binary_Proto_Buffer_WhenAll      | 5000         |  32.926 ms |  1.0832 ms |  3.1080 ms |  32.836 ms |  384.6154 |  307.6923 |         - |  2475.03 KB |
| Binary_Proto_WhenAll             | 5000         |  33.454 ms |  2.0901 ms |  6.0303 ms |  35.536 ms |  400.0000 |  200.0000 |         - |  2481.26 KB |
| Confluent_Proto_WhenAll          | 5000         |  37.963 ms |  0.7509 ms |  2.0681 ms |  37.480 ms | 1000.0000 |  666.6667 |         - |  6652.58 KB |
| Binary_Proto_Buffer_MessageBatch | 10000        |  52.968 ms |  1.5768 ms |  4.3955 ms |  52.938 ms |  400.0000 |         - |         - |   2813.2 KB |
| Binary_Proto_MessageBatch        | 10000        |  53.077 ms |  1.5045 ms |  4.2924 ms |  52.416 ms |  400.0000 |         - |         - |   2813.2 KB |
| Binary_SpanJson_MessageBatch     | 5000         |  57.299 ms |  2.1488 ms |  6.1997 ms |  56.774 ms |         - |         - |         - |  1407.25 KB |
| Binary_Json_Buffer_MessageBatch  | 5000         |  59.708 ms |  1.4510 ms |  3.9720 ms |  59.844 ms |  500.0000 |         - |         - |  3516.85 KB |
| Binary_Proto_Buffer_WhenAll      | 10000        |  60.708 ms |  1.9994 ms |  5.6066 ms |  60.399 ms |  666.6667 |  444.4444 |  222.2222 |  4875.34 KB |
| Binary_Proto_WhenAll             | 10000        |  61.909 ms |  1.4580 ms |  4.1596 ms |  61.366 ms |  666.6667 |  444.4444 |  222.2222 |  4870.61 KB |
| Confluent_Proto_WhenAll          | 10000        |  75.649 ms |  3.0364 ms |  8.6138 ms |  75.656 ms | 2000.0000 | 1000.0000 |  666.6667 | 13304.45 KB |
| Confluent_Json_WhenAll           | 5000         |  82.425 ms |  6.3086 ms | 18.5021 ms |  79.052 ms | 1000.0000 |         - |         - |  9583.16 KB |
| Binary_SpanJson_MessageBatch     | 10000        | 107.249 ms |  2.9770 ms |  8.2988 ms | 108.671 ms |         - |         - |         - |  2814.13 KB |
| Binary_Json_Buffer_MessageBatch  | 10000        | 113.456 ms |  5.0016 ms | 13.9425 ms | 114.078 ms | 1000.0000 |         - |         - |  7033.18 KB |
| Confluent_Json_WhenAll           | 10000        | 145.569 ms |  4.2178 ms | 12.3034 ms | 142.682 ms | 3000.0000 | 1000.0000 |         - | 19164.77 KB |
| Binary_Proto_Buffer_MessageBatch | 30000        | 147.694 ms |  6.7160 ms | 18.8323 ms | 148.545 ms | 1000.0000 |         - |         - |  8438.85 KB |
| Binary_Proto_MessageBatch        | 30000        | 150.891 ms |  7.2669 ms | 20.2573 ms | 149.965 ms | 1250.0000 |         - |         - |  8438.59 KB |
| Binary_Proto_WhenAll             | 30000        | 173.801 ms |  6.6339 ms | 18.8193 ms | 173.969 ms | 2000.0000 | 1000.0000 |         - | 14752.95 KB |
| Binary_Proto_Buffer_WhenAll      | 30000        | 175.960 ms |  6.1702 ms | 17.3019 ms | 173.881 ms | 2000.0000 | 1000.0000 |         - | 14287.72 KB |
| Confluent_Proto_WhenAll          | 30000        | 219.556 ms |  8.3020 ms | 23.8200 ms | 220.207 ms | 6000.0000 | 2000.0000 | 1000.0000 |  39656.1 KB |
| Binary_Json_Buffer_MessageBatch  | 30000        | 335.746 ms | 11.5667 ms | 33.1872 ms | 335.233 ms | 3000.0000 |         - |         - | 21096.23 KB |
| Binary_SpanJson_MessageBatch     | 30000        | 339.693 ms | 16.4397 ms | 45.2798 ms | 328.574 ms | 1000.0000 |         - |         - |   8438.8 KB |
| Confluent_Json_WhenAll           | 30000        | 433.557 ms |  9.5086 ms | 27.4346 ms | 429.762 ms | 9500.0000 | 3000.0000 | 1500.0000 | 57237.13 KB |



Thanks to [@xeromorph](https://github.com/xeromorph) for the great ideas and help.