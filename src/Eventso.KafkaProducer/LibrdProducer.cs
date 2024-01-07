using System.Runtime.InteropServices;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Impl;

namespace Eventso.KafkaProducer;

internal static class LibrdProducer
{
    private const int StackThreshold = 256;
    // ReSharper disable once UseArrayEmptyMethod
    private static readonly byte[] EmptyByteArray = new byte[0];

    public static ErrorCode Produce(
        SafeKafkaHandle handle,
        string topic,
        ReadOnlySpan<byte> val,
        ReadOnlySpan<byte> key,
        int partition,
        long timestamp,
        IReadOnlyList<IHeader>? headers,
        IntPtr opaque)
    {
        var pValue = IntPtr.Zero;
        var pKey = IntPtr.Zero;

        var gchValue = default(GCHandle);
        var gchKey = default(GCHandle);

        if (val.Length == 0 && val != ReadOnlySpan<byte>.Empty)
        {
            gchValue = GCHandle.Alloc(EmptyByteArray, GCHandleType.Pinned);
            pValue = gchValue.AddrOfPinnedObject();
        }

        if (key.Length == 0 && key != ReadOnlySpan<byte>.Empty)
        {
            gchKey = GCHandle.Alloc(EmptyByteArray, GCHandleType.Pinned);
            pKey = gchKey.AddrOfPinnedObject();
        }

        IntPtr headersPtr = MarshalHeaders(handle, headers);

        try
        {
            ErrorCode errorCode;

            unsafe
            {
                fixed (byte* keyPtr = key, valPtr = val)
                {
                    errorCode = Librdkafka.produceva(
                        handle.DangerousGetHandle(),
                        topic,
                        partition,
                        (IntPtr)MsgFlags.MSG_F_COPY,
                        pValue == IntPtr.Zero ? (IntPtr)valPtr : pValue, (UIntPtr)val.Length,
                        pKey == IntPtr.Zero ? (IntPtr)keyPtr : pKey, (UIntPtr)key.Length,
                        timestamp,
                        headersPtr,
                        opaque);
                }
            }

            if (errorCode != ErrorCode.NoError)
            {
                if (headersPtr != IntPtr.Zero)
                    Librdkafka.headers_destroy(headersPtr);
            }

            return errorCode;
        }
        catch
        {
            if (headersPtr != IntPtr.Zero)
                Librdkafka.headers_destroy(headersPtr);

            throw;
        }
        finally
        {
            if (gchValue.IsAllocated)
                gchValue.Free();

            if (gchKey.IsAllocated)
                gchKey.Free();
        }
    }

    private static IntPtr MarshalHeaders(SafeKafkaHandle handle, IReadOnlyList<IHeader>? headers)
    {
        if (headers == null || headers.Count <= 0)
            return IntPtr.Zero;

        var headersPtr = Librdkafka.headers_new((IntPtr)headers.Count);

        if (headersPtr == IntPtr.Zero)
            throw new Exception("Failed to create headers list.");

        for (var x = 0; x < headers.Count; x++)
        {
            var header = headers[x];

            if (header.Key == null)
                throw new ArgumentNullException("Message header keys must not be null.");

            var err = MarshalHeader(headersPtr, header);

            if (err != ErrorCode.NoError)
                throw new KafkaException(handle.CreatePossiblyFatalError(err, null));
        }

        return headersPtr;
    }

    private static ErrorCode MarshalHeader(IntPtr headersPtr, IHeader header)
    {
        var keyLength = Encoding.UTF8.GetByteCount(header.Key);
        var keyBytes = keyLength <= StackThreshold ? stackalloc byte[keyLength] : new byte[keyLength];

        Encoding.UTF8.GetBytes(header.Key, keyBytes);

        var valueBytes = header.GetValueBytes();

        unsafe
        {
            fixed (byte* pKey = keyBytes, pValue = valueBytes)
            {
                return Librdkafka.headers_add(
                    headersPtr,
                    (IntPtr)pKey, (IntPtr)keyBytes.Length,
                    (IntPtr)pValue, (IntPtr)(valueBytes == null ? 0 : valueBytes.Length));
            }
        }
    }
}