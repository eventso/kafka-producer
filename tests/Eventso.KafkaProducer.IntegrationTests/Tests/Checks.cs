using System.Runtime.InteropServices;
using FluentAssertions;
using Xunit;

namespace Eventso.KafkaProducer.IntegrationTests.Tests;

public class Checks
{
    [Fact]
    public void Duplicated_GCHandles()
    {
        var obj = new TaskCompletionSource();

        var h1 = GCHandle.Alloc(obj);
        var h2 = GCHandle.Alloc(obj);
        var h2Ptr = GCHandle.ToIntPtr(h2);

        h1.Equals(h2).Should().BeFalse();

        h1.Free();

        h1.IsAllocated.Should().BeFalse();

        h2.IsAllocated.Should().BeTrue();

        var h2Restored = GCHandle.FromIntPtr(h2Ptr);

        h2Restored.Target.Should().BeSameAs(obj);
    }
}