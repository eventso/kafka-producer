using System.Text.Json;
using AutoFixture;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using CommunityToolkit.HighPerformance.Buffers;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Eventso.KafkaProducer.Json;
using Eventso.KafkaProducer.Protobuf;
using Google.Protobuf;
using Tests;

namespace Eventso.KafkaProducer.Benchmark;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
public class Producing
{
    private IProducer<long, OrderProto> confluentProducerProto = null!;
    private IProducer<long, Order> confluentProducerJson = null!;
    private IProducer binaryProducer = null!;
    private const string Topic = "benchmark-topic";

    [GlobalSetup]
    public async Task Setup()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "127.0.0.1:9092",
            QueueBufferingMaxMessages = 2000000,
            MessageSendMaxRetries = 3,
            RetryBackoffMs = 500,
            LingerMs = 1
        };

        confluentProducerProto = new ProducerBuilder<long, OrderProto>(config)
            .SetValueSerializer(new ProtoSerializer())
            .Build();

        confluentProducerJson = new DependentProducerBuilder<long, Order>(confluentProducerProto.Handle)
            .SetValueSerializer(new KafkaJsonSerializer())
            .Build();

        binaryProducer = confluentProducerProto.CreateBinary();

        var adminClient = new DependentAdminClientBuilder(confluentProducerProto.Handle).Build();
        await adminClient.CreateTopicsAsync(new List<TopicSpecification>
        {
            new() { Name = Topic, NumPartitions = 1, ReplicationFactor = 1 }
        });
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        var adminClient = new DependentAdminClientBuilder(confluentProducerProto.Handle).Build();
        await adminClient.DeleteTopicsAsync(new[] { Topic });
    }

    [Benchmark]
    [ArgumentsSource(nameof(Data))]
    public Task Confluent_Proto_WhenAll(IEnumerable<OrderProto> records)
    {
        return Task.WhenAll(records.Select(r
            => confluentProducerProto.ProduceAsync(Topic, new Message<long, OrderProto> { Key = 123456L, Value = r })));
    }

    [Benchmark]
    [ArgumentsSource(nameof(DataJson))]
    public Task Confluent_Json_WhenAll(IEnumerable<Order> records)
    {
        return Task.WhenAll(records.Select(r
            => confluentProducerJson.ProduceAsync(Topic, new Message<long, Order> { Key = 123456L, Value = r })));
    }

    [Benchmark]
    [ArgumentsSource(nameof(Data))]
    public Task Binary_Proto_WhenAll(IEnumerable<OrderProto> records)
    {
        return Task.WhenAll(records.Select(r => binaryProducer.ProduceAsync(Topic, 123456L, r)));
    }

    [Benchmark]
    [ArgumentsSource(nameof(Data))]
    public async Task Binary_Proto_Buffer_WhenAll(IEnumerable<OrderProto> records)
    {
        using var buffer = new ArrayPoolBufferWriter<byte>();

        await Task.WhenAll(records.Select(r => binaryProducer.ProduceAsync(Topic, 123456L, r, buffer)));
    }

    [Benchmark]
    [ArgumentsSource(nameof(Data))]
    public async Task Binary_Proto_MessageBatch(IEnumerable<OrderProto> records)
    {
        var batch = binaryProducer.CreateBatch(Topic);
        foreach (var record in records)
            batch.Produce(123456L, record);

        await batch.Complete();
    }

    [Benchmark]
    [ArgumentsSource(nameof(Data))]
    public async Task Binary_Proto_Buffer_MessageBatch(IEnumerable<OrderProto> records)
    {
        var batch = binaryProducer.CreateBatch(Topic);
        foreach (var record in records)
            batch.Produce(123456L, record, batch.GetBuffer());

        await batch.Complete();
    }

    [Benchmark]
    [ArgumentsSource(nameof(DataJson))]
    public async Task Binary_Json_Buffer_MessageBatch(IEnumerable<Order> records)
    {
        var batch = binaryProducer.CreateBatch(Topic);
        foreach (var record in records)
            batch.ProduceJson(123456L, record, batch.GetBuffer());

        await batch.Complete();
    }

    [Benchmark]
    [ArgumentsSource(nameof(DataJson))]
    public async Task Binary_SpanJson_MessageBatch(IEnumerable<Order> records)
    {
        var batch = binaryProducer.CreateBatch(Topic);
        foreach (var record in records)
            Eventso.KafkaProducer.SpanJson.JsonValueExtensions.ProduceJson(batch, 123456L, record);

        await batch.Complete();
    }

    public IEnumerable<OrderProto[]> Data()
    {
        var fixture = new Fixture { RepeatCount = 5 };

        yield return fixture.CreateMany<OrderProto>(100).ToArray();
        yield return fixture.CreateMany<OrderProto>(1000).ToArray();
        yield return fixture.CreateMany<OrderProto>(10000).ToArray();
    }

    public IEnumerable<Order[]> DataJson()
    {
        var fixture = new Fixture { RepeatCount = 5 };

        yield return fixture.CreateMany<Order>(100).ToArray();
        yield return fixture.CreateMany<Order>(1000).ToArray();
        yield return fixture.CreateMany<Order>(10000).ToArray();
    }

    private class ProtoSerializer : ISerializer<OrderProto>
    {
        public byte[] Serialize(OrderProto data, SerializationContext context)
            => data.ToByteArray();
    }

    private class KafkaJsonSerializer : ISerializer<Order>
    {
        public byte[] Serialize(Order data, SerializationContext context)
            => JsonSerializer.SerializeToUtf8Bytes(data);
    }

    public sealed record Order(long Id, string UserName, Order.Item[] Items)
    {
        public sealed record Item(long Id, string Name, int Quantity, int SellerId, double Discount, double Price);
    }
}