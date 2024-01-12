// Copyright 2019 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using System.Buffers.Binary;
using System.Runtime.InteropServices;

namespace Eventso.KafkaProducer.IntegrationTests
{
    class SimpleAsyncSerializer : IAsyncSerializer<string>
    {
        public async Task<byte[]> SerializeAsync(string data, SerializationContext context)
        {
            await Task.Delay(500).ConfigureAwait(false);
            return Serializers.Utf8.Serialize(data, context);
        }

        public ISerializer<string> SyncOverAsync()
        {
            return new SyncOverAsyncSerializer<string>(this);
        }
    }

    class SimpleSyncSerializer : ISerializer<string>
    {
        public byte[] Serialize(string data, SerializationContext context)
        {
            Thread.Sleep(500);
            return Serializers.Utf8.Serialize(data, context);
        }
    }

    public sealed class ShortDeserializer : IDeserializer<short>
    {
        public short Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            return BinaryPrimitives.ReadInt16BigEndian(data);
        }
    }

    public sealed class GuidDeserializer : IDeserializer<Guid>
    {
        public Guid Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
#if NET8_0_OR_GREATER
                        return new Guid(data, bigEndian: true);
#else
            return new GuidRaw
            {
                Data1 = BinaryPrimitives.ReadInt32BigEndian(data),
                Data2 = BinaryPrimitives.ReadInt16BigEndian(data[4..]),
                Data3 = BinaryPrimitives.ReadInt16BigEndian(data[6..]),
                Data4 = BitConverter.IsLittleEndian
                    ? BinaryPrimitives.ReverseEndianness(BinaryPrimitives.ReadInt64BigEndian(data[8..]))
                    : BinaryPrimitives.ReadInt64BigEndian(data[8..])
            }.Value;
#endif
        }

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
    }
}
