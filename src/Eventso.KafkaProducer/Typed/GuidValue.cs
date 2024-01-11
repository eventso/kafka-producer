using System.Buffers.Binary;
using System.Runtime.InteropServices;

namespace Eventso.KafkaProducer;

public readonly struct GuidValue(Guid value) : IBinarySerializable
{
    public int GetSize() => 16;

    public int WriteBytes(Span<byte> destination)
    {
        // source https://github.com/npgsql/npgsql/blob/main/src/Npgsql/Internal/Converters/Primitive/GuidUuidConverter.cs
#if NET8_0_OR_GREATER
        value.TryWriteBytes(destination, bigEndian: true, out _);
#else
        var raw = new GuidRaw(value);
        BinaryPrimitives.WriteInt32BigEndian(destination, raw.Data1);
        BinaryPrimitives.WriteInt16BigEndian(destination[4..], raw.Data2);
        BinaryPrimitives.WriteInt16BigEndian(destination[6..], raw.Data3);
        BinaryPrimitives.WriteInt64BigEndian(
            destination[8..],
            BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(raw.Data4) : raw.Data4);
#endif

        return GetSize();
    }

    public static implicit operator GuidValue(Guid value) => new(value);

#if !NET8_0_OR_GREATER
    [StructLayout(LayoutKind.Explicit)]
    struct GuidRaw
    {
        [FieldOffset(0)] public Guid Value;
        [FieldOffset(0)] public int Data1;
        [FieldOffset(4)] public short Data2;
        [FieldOffset(6)] public short Data3;
        [FieldOffset(8)] public long Data4;
        public GuidRaw(Guid value) : this() => Value = value;
    }
#endif
}