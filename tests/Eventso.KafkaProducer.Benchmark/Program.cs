using BenchmarkDotNet.Running;

namespace Eventso.KafkaProducer.Benchmark;

class Program
{
    static void Main(string[] args)
    {
        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}