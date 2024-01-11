using System.Text;

namespace Eventso.KafkaProducer;

public readonly struct StringValue(string? value, Encoding encoding) : IBinarySerializable
{
    public StringValue(string? value)
        : this(value, Encoding.UTF8)
    {
    }

    public int GetSize() => value == null ? 0 : encoding.GetMaxByteCount(value.Length);

    public int WriteBytes(Span<byte> destination)
    {
        if (value != null)
            return encoding.GetBytes(value, destination);
        return 0;
    }

    public static implicit operator StringValue(string? value) => new(value);
}