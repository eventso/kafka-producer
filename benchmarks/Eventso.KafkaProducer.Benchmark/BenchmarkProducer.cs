using System.Text.Json;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using CommunityToolkit.HighPerformance.Buffers;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
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
    private OrderProto orderProto = null!;
    private Order order = null!;
    private const string Topic = "benchmark-topic";

    [Params(1000, 5000, 10000, 30000)]
    public int MessageCount { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "127.0.0.1:9092",
            QueueBufferingMaxMessages = 2000000,
            LingerMs = 0,
        };

        confluentProducerProto = new ProducerBuilder<long, OrderProto>(config)
            .SetValueSerializer(new ProtoSerializer())
            .Build();

        confluentProducerJson = new DependentProducerBuilder<long, Order>(confluentProducerProto.Handle)
            .SetValueSerializer(new KafkaJsonSerializer())
            .Build();

        binaryProducer = confluentProducerProto.CreateBinary();

        var adminClient = new DependentAdminClientBuilder(confluentProducerProto.Handle).Build();
        try
        {
            await adminClient.CreateTopicsAsync(new List<TopicSpecification>
            {
                new() { Name = Topic, NumPartitions = 3, ReplicationFactor = 1 }
            });
        }
        catch (CreateTopicsException)
        {
        }

        orderProto = new OrderProto
        {
            Id = Int64.MaxValue,
            UserName = Guid.NewGuid().ToString(),
            Items =
            {
                Enumerable.Repeat(
                    new OrderProto.Types.Item()
                    {
                        Id = Int64.MaxValue,
                        Name = Guid.NewGuid().ToString(),
                        Discount = Double.MaxValue,
                        Price = Double.MaxValue,
                        Quantity = Int32.MaxValue,
                        SellerId = Int32.MaxValue
                    }, 3)
            }
        };

        order = new Order(
            Id: Int64.MaxValue,
            UserName: Guid.NewGuid().ToString(),
            Items:
            Enumerable.Repeat(
                new Order.Item(
                    Id: Int64.MaxValue,
                    Name: Guid.NewGuid().ToString(),
                    Discount: Double.MaxValue,
                    Price: Double.MaxValue,
                    Quantity: Int32.MaxValue,
                    SellerId: Int32.MaxValue), 3).ToArray());
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        var adminClient = new DependentAdminClientBuilder(confluentProducerProto.Handle).Build();
        await adminClient.DeleteTopicsAsync(new[] { Topic });
    }

    [Benchmark]
    public Task Confluent_Proto_WhenAll()
    {
        var records = Enumerable.Repeat(orderProto, MessageCount);

        return Task.WhenAll(records.Select(r
            => confluentProducerProto.ProduceAsync(Topic, new Message<long, OrderProto> { Key = 123456L, Value = r })));
    }

    [Benchmark]
    public Task Confluent_Json_WhenAll()
    {
        var records = Enumerable.Repeat(order, MessageCount);

        return Task.WhenAll(records.Select(r
            => confluentProducerJson.ProduceAsync(Topic, new Message<long, Order> { Key = 123456L, Value = r })));
    }

    [Benchmark]
    public Task Binary_Proto_WhenAll()
    {
        var records = Enumerable.Repeat(orderProto, MessageCount);
        return Task.WhenAll(records.Select(r => binaryProducer.ProduceAsync(Topic, 123456L, r)));
    }

    [Benchmark]
    public async Task Binary_Proto_Buffer_WhenAll()
    {
        var records = Enumerable.Repeat(orderProto, MessageCount);
        using var buffer = new ArrayPoolBufferWriter<byte>();

        await Task.WhenAll(records.Select(r => binaryProducer.ProduceAsync(Topic, 123456L, r, buffer)));
    }

    [Benchmark]
    public async Task Binary_Proto_MessageBatch()
    {
        var records = Enumerable.Repeat(orderProto, MessageCount);
        var batch = binaryProducer.CreateBatch(Topic);
        var buffer = batch.GetBuffer();

        foreach (var record in records)
            batch.Produce<LongValue, ProtoValue>(123456L, new(record));

        await batch.Complete();
    }

    [Benchmark]
    public async Task Binary_Proto_Buffer_MessageBatch()
    {
        var records = Enumerable.Repeat(orderProto, MessageCount);
        var batch = binaryProducer.CreateBatch(Topic);
        var buffer = batch.GetBuffer();

        foreach (var record in records)
            batch.Produce(123456L, record, buffer);
        await batch.Complete();
    }

    [Benchmark]
    public async Task Binary_Json_Buffer_MessageBatch()
    {
        var records = Enumerable.Repeat(order, MessageCount);
        var batch = binaryProducer.CreateBatch(Topic);
        var buffer = batch.GetBuffer();
        foreach (var record in records)
        {
            batch.ProduceJson(123456L, record, buffer);
        }

        await batch.Complete();
    }

    [Benchmark]
    public async Task Binary_SpanJson_MessageBatch()
    {
        var records = Enumerable.Repeat(order, MessageCount);
        var batch = binaryProducer.CreateBatch(Topic);
        foreach (var record in records)
            SpanJsonValueExtensions.ProduceJson(batch, 123456L, record);
        
        await batch.Complete();
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