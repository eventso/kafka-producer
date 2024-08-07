﻿using System.Buffers.Binary;

namespace Eventso.KafkaProducer;

public readonly struct IntValue(int value) : IBinarySerializable
{
    public int Size => sizeof(int);

    public int WriteBytes(Span<byte> destination)
    {
        BinaryPrimitives.WriteInt32BigEndian(destination, value);
        return Size;
    }

    public static implicit operator IntValue(int value) => new(value);
}