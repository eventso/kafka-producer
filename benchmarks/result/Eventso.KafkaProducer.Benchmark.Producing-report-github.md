```

BenchmarkDotNet v0.13.12, Windows 10 (10.0.19045.3930/22H2/2022Update)
11th Gen Intel Core i7-1185G7 3.00GHz, 1 CPU, 8 logical and 4 physical cores
.NET SDK 8.0.101
  [Host]     : .NET 8.0.1 (8.0.123.58001), X64 RyuJIT AVX-512F+CD+BW+DQ+VL+VBMI
  DefaultJob : .NET 8.0.1 (8.0.123.58001), X64 RyuJIT AVX-512F+CD+BW+DQ+VL+VBMI


```
| Method                           | MessageCount | Mean       | Error      | StdDev     | Median     | Gen0      | Gen1      | Gen2      | Allocated   |
|--------------------------------- |------------- |-----------:|-----------:|-----------:|-----------:|----------:|----------:|----------:|------------:|
| Binary_Proto_Buffer_MessageBatch | 1000         |   9.353 ms |  0.2653 ms |  0.7306 ms |   9.297 ms |   31.2500 |         - |         - |   281.76 KB |
| Binary_Proto_MessageBatch        | 1000         |   9.722 ms |  0.3157 ms |  0.8851 ms |   9.669 ms |   31.2500 |         - |         - |   281.76 KB |
| Binary_Proto_WhenAll             | 1000         |  11.274 ms |  0.3988 ms |  1.1312 ms |  11.157 ms |   62.5000 |         - |         - |   493.02 KB |
| Binary_Proto_Buffer_WhenAll      | 1000         |  11.880 ms |  0.4948 ms |  1.3957 ms |  11.877 ms |   71.4286 |         - |         - |   493.35 KB |
| Confluent_Proto_WhenAll          | 1000         |  12.996 ms |  0.3991 ms |  1.1643 ms |  12.752 ms |  187.5000 |   93.7500 |         - |  1321.33 KB |
| Binary_SpanJson_MessageBatch     | 1000         |  15.136 ms |  0.3758 ms |  1.0413 ms |  15.113 ms |   31.2500 |         - |         - |   281.66 KB |
| Binary_Json_Buffer_MessageBatch  | 1000         |  17.775 ms |  0.7340 ms |  2.1061 ms |  17.934 ms |   93.7500 |         - |         - |   703.67 KB |
| Confluent_Json_WhenAll           | 1000         |  20.693 ms |  0.6509 ms |  1.8884 ms |  20.763 ms |  281.2500 |  156.2500 |         - |  1907.29 KB |
| Binary_Proto_Buffer_MessageBatch | 5000         |  29.038 ms |  0.5780 ms |  1.5726 ms |  28.955 ms |  218.7500 |         - |         - |  1406.83 KB |
| Binary_Proto_MessageBatch        | 5000         |  29.490 ms |  0.7871 ms |  2.2329 ms |  29.511 ms |  200.0000 |         - |         - |  1407.03 KB |
| Binary_Proto_Buffer_WhenAll      | 5000         |  32.926 ms |  1.0832 ms |  3.1080 ms |  32.836 ms |  384.6154 |  307.6923 |         - |  2475.03 KB |
| Binary_Proto_WhenAll             | 5000         |  33.454 ms |  2.0901 ms |  6.0303 ms |  35.536 ms |  400.0000 |  200.0000 |         - |  2481.26 KB |
| Confluent_Proto_WhenAll          | 5000         |  37.963 ms |  0.7509 ms |  2.0681 ms |  37.480 ms | 1000.0000 |  666.6667 |         - |  6652.58 KB |
| Binary_Proto_Buffer_MessageBatch | 10000        |  52.968 ms |  1.5768 ms |  4.3955 ms |  52.938 ms |  400.0000 |         - |         - |   2813.2 KB |
| Binary_Proto_MessageBatch        | 10000        |  53.077 ms |  1.5045 ms |  4.2924 ms |  52.416 ms |  400.0000 |         - |         - |   2813.2 KB |
| Binary_SpanJson_MessageBatch     | 5000         |  57.299 ms |  2.1488 ms |  6.1997 ms |  56.774 ms |         - |         - |         - |  1407.25 KB |
| Binary_Json_Buffer_MessageBatch  | 5000         |  59.708 ms |  1.4510 ms |  3.9720 ms |  59.844 ms |  500.0000 |         - |         - |  3516.85 KB |
| Binary_Proto_Buffer_WhenAll      | 10000        |  60.708 ms |  1.9994 ms |  5.6066 ms |  60.399 ms |  666.6667 |  444.4444 |  222.2222 |  4875.34 KB |
| Binary_Proto_WhenAll             | 10000        |  61.909 ms |  1.4580 ms |  4.1596 ms |  61.366 ms |  666.6667 |  444.4444 |  222.2222 |  4870.61 KB |
| Confluent_Proto_WhenAll          | 10000        |  75.649 ms |  3.0364 ms |  8.6138 ms |  75.656 ms | 2000.0000 | 1000.0000 |  666.6667 | 13304.45 KB |
| Confluent_Json_WhenAll           | 5000         |  82.425 ms |  6.3086 ms | 18.5021 ms |  79.052 ms | 1000.0000 |         - |         - |  9583.16 KB |
| Binary_SpanJson_MessageBatch     | 10000        | 107.249 ms |  2.9770 ms |  8.2988 ms | 108.671 ms |         - |         - |         - |  2814.13 KB |
| Binary_Json_Buffer_MessageBatch  | 10000        | 113.456 ms |  5.0016 ms | 13.9425 ms | 114.078 ms | 1000.0000 |         - |         - |  7033.18 KB |
| Confluent_Json_WhenAll           | 10000        | 145.569 ms |  4.2178 ms | 12.3034 ms | 142.682 ms | 3000.0000 | 1000.0000 |         - | 19164.77 KB |
| Binary_Proto_Buffer_MessageBatch | 30000        | 147.694 ms |  6.7160 ms | 18.8323 ms | 148.545 ms | 1000.0000 |         - |         - |  8438.85 KB |
| Binary_Proto_MessageBatch        | 30000        | 150.891 ms |  7.2669 ms | 20.2573 ms | 149.965 ms | 1250.0000 |         - |         - |  8438.59 KB |
| Binary_Proto_WhenAll             | 30000        | 173.801 ms |  6.6339 ms | 18.8193 ms | 173.969 ms | 2000.0000 | 1000.0000 |         - | 14752.95 KB |
| Binary_Proto_Buffer_WhenAll      | 30000        | 175.960 ms |  6.1702 ms | 17.3019 ms | 173.881 ms | 2000.0000 | 1000.0000 |         - | 14287.72 KB |
| Confluent_Proto_WhenAll          | 30000        | 219.556 ms |  8.3020 ms | 23.8200 ms | 220.207 ms | 6000.0000 | 2000.0000 | 1000.0000 |  39656.1 KB |
| Binary_Json_Buffer_MessageBatch  | 30000        | 335.746 ms | 11.5667 ms | 33.1872 ms | 335.233 ms | 3000.0000 |         - |         - | 21096.23 KB |
| Binary_SpanJson_MessageBatch     | 30000        | 339.693 ms | 16.4397 ms | 45.2798 ms | 328.574 ms | 1000.0000 |         - |         - |   8438.8 KB |
| Confluent_Json_WhenAll           | 30000        | 433.557 ms |  9.5086 ms | 27.4346 ms | 429.762 ms | 9500.0000 | 3000.0000 | 1500.0000 | 57237.13 KB |
