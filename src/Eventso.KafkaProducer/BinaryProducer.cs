using System.Runtime.InteropServices;
using Confluent.Kafka;
using Confluent.Kafka.Impl;

namespace Eventso.KafkaProducer;

internal static class BinaryProducer
{
    public static void Produce(
        SafeKafkaHandle handle,
        string topic,
        ReadOnlySpan<byte> val,
        ReadOnlySpan<byte> key,
        Timestamp timestamp,
        Partition partition,
        IReadOnlyList<IHeader>? headers,
        IDeliveryHandler? deliveryHandler)
    {
        if (timestamp.Type != TimestampType.CreateTime)
        {
            if (timestamp != Timestamp.Default)
            {
                throw new ArgumentException("Timestamp must be either Timestamp.Default, or Timestamp.CreateTime.");
            }
        }

        ErrorCode err;
        if (deliveryHandler != null)
        {
            // Passes the TaskCompletionSource to the delivery report callback via the msg_opaque pointer

            // Note: There is a level of indirection between the GCHandle and
            // physical memory address. GCHandle.ToIntPtr doesn't get the
            // physical address, it gets an id that refers to the object via
            // a handle-table.
            var gch = GCHandle.Alloc(deliveryHandler);
            var ptr = GCHandle.ToIntPtr(gch);

            err = LibrdProducer.Produce(
                handle,
                topic,
                val,
                key,
                partition.Value,
                timestamp.UnixTimestampMs,
                headers,
                ptr);

            if (err != ErrorCode.NoError)
            {
                // note: freed in the delivery handler callback otherwise.
                gch.Free();
            }
        }
        else
        {
            err = LibrdProducer.Produce(
                handle,
                topic,
                val,
                key,
                partition.Value,
                timestamp.UnixTimestampMs,
                headers,
                IntPtr.Zero);
        }

        if (err != ErrorCode.NoError)
        {
            throw new KafkaException(handle.CreatePossiblyFatalError(err, null));
        }
    }
}