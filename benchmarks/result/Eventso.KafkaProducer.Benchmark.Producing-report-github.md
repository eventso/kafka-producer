```

BenchmarkDotNet v0.13.12, Windows 10 (10.0.19045.3930/22H2/2022Update)
11th Gen Intel Core i7-1185G7 3.00GHz, 1 CPU, 8 logical and 4 physical cores
.NET SDK 8.0.101
  [Host]     : .NET 8.0.1 (8.0.123.58001), X64 RyuJIT AVX-512F+CD+BW+DQ+VL+VBMI
  DefaultJob : .NET 8.0.1 (8.0.123.58001), X64 RyuJIT AVX-512F+CD+BW+DQ+VL+VBMI


```
| Method                           | MessageCount | Mean       | Error      | StdDev     | Median     | Gen0       | Gen1      | Gen2      | Allocated   |
|--------------------------------- |------------- |-----------:|-----------:|-----------:|-----------:|-----------:|----------:|----------:|------------:|
| Binary_Proto_Buffer_MessageBatch | 1000         |   9.182 ms |  0.3009 ms |  0.8537 ms |   9.091 ms |    31.2500 |         - |         - |   281.76 KB |
| Binary_Proto_MessageBatch        | 1000         |   9.220 ms |  0.2548 ms |  0.6890 ms |   9.221 ms |    31.2500 |         - |         - |   281.76 KB |
| Binary_Proto_WhenAll             | 1000         |   9.797 ms |  0.4949 ms |  1.3877 ms |   9.492 ms |    93.7500 |   31.2500 |         - |   594.54 KB |
| Binary_Proto_Buffer_WhenAll      | 1000         |  10.339 ms |  0.4960 ms |  1.3908 ms |   9.997 ms |    93.7500 |   15.6250 |         - |   594.65 KB |
| Confluent_Proto_WhenAll          | 1000         |  10.979 ms |  0.3478 ms |  0.9638 ms |  10.805 ms |   203.1250 |  125.0000 |         - |  1321.31 KB |
| Binary_SpanJson_MessageBatch     | 1000         |  13.834 ms |  0.3492 ms |  0.9907 ms |  13.720 ms |    31.2500 |         - |         - |   281.66 KB |
| Binary_Json_Buffer_MessageBatch  | 1000         |  14.813 ms |  0.2952 ms |  0.7829 ms |  14.769 ms |   109.3750 |         - |         - |   703.66 KB |
| Confluent_Json_WhenAll           | 1000         |  16.791 ms |  0.4752 ms |  1.3787 ms |  16.547 ms |   281.2500 |  156.2500 |         - |  1907.29 KB |
| Binary_Proto_Buffer_MessageBatch | 5000         |  26.799 ms |  0.8270 ms |  2.3994 ms |  26.711 ms |   218.7500 |         - |         - |  1406.83 KB |
| Binary_Proto_MessageBatch        | 5000         |  27.673 ms |  0.5426 ms |  1.0454 ms |  27.736 ms |   218.7500 |         - |         - |  1406.83 KB |
| Binary_Proto_Buffer_WhenAll      | 5000         |  29.296 ms |  0.9628 ms |  2.7471 ms |  29.155 ms |   437.5000 |  312.5000 |         - |  2971.25 KB |
| Binary_Proto_WhenAll             | 5000         |  31.977 ms |  0.6759 ms |  1.9285 ms |  31.589 ms |   428.5714 |  285.7143 |         - |  2976.32 KB |
| Confluent_Proto_WhenAll          | 5000         |  36.196 ms |  0.9259 ms |  2.5810 ms |  35.691 ms |  1000.0000 |  666.6667 |         - |  6652.57 KB |
| Binary_Proto_Buffer_MessageBatch | 10000        |  49.588 ms |  1.8133 ms |  5.1145 ms |  49.277 ms |   333.3333 |         - |         - |  2813.28 KB |
| Binary_Proto_MessageBatch        | 10000        |  51.473 ms |  1.5911 ms |  4.6414 ms |  50.436 ms |   454.5455 |         - |         - |  2813.21 KB |
| Binary_SpanJson_MessageBatch     | 5000         |  52.179 ms |  1.4200 ms |  4.0052 ms |  51.450 ms |   200.0000 |         - |         - |  1406.82 KB |
| Binary_Proto_WhenAll             | 10000        |  55.395 ms |  2.1656 ms |  6.2827 ms |  55.583 ms |   900.0000 |  400.0000 |  200.0000 |  5850.93 KB |
| Binary_Json_Buffer_MessageBatch  | 5000         |  56.548 ms |  1.7592 ms |  4.9620 ms |  56.224 ms |   500.0000 |         - |         - |  3516.55 KB |
| Binary_Proto_Buffer_WhenAll      | 10000        |  59.476 ms |  2.0294 ms |  5.7572 ms |  59.633 ms |   750.0000 |  250.0000 |         - |  5890.76 KB |
| Confluent_Proto_WhenAll          | 10000        |  70.414 ms |  1.3629 ms |  3.1587 ms |  69.853 ms |  2000.0000 | 1000.0000 |  500.0000 |  13304.7 KB |
| Confluent_Json_WhenAll           | 5000         |  70.712 ms |  3.1865 ms |  9.1938 ms |  67.757 ms |  1000.0000 |         - |         - |  9583.21 KB |
| Binary_SpanJson_MessageBatch     | 10000        | 100.186 ms |  2.5342 ms |  7.2303 ms |  99.433 ms |   333.3333 |         - |         - |  2813.19 KB |
| Binary_Json_Buffer_MessageBatch  | 10000        | 109.274 ms |  2.7443 ms |  7.5587 ms | 108.994 ms |  1000.0000 |         - |         - |  7033.14 KB |
| Confluent_Json_WhenAll           | 10000        | 117.574 ms |  4.3187 ms | 12.3216 ms | 115.484 ms |  3000.0000 | 1000.0000 |         - | 19164.77 KB |
| Binary_Proto_Buffer_MessageBatch | 30000        | 137.687 ms |  4.2871 ms | 12.2314 ms | 135.332 ms |  1250.0000 |         - |         - |   8438.6 KB |
| Binary_Proto_MessageBatch        | 30000        | 137.800 ms |  4.4953 ms | 12.8977 ms | 134.260 ms |  1250.0000 |         - |         - |  8438.61 KB |
| Binary_Proto_Buffer_WhenAll      | 30000        | 153.744 ms |  4.4629 ms | 12.4409 ms | 153.000 ms |  2500.0000 | 1000.0000 |  500.0000 | 17230.07 KB |
| Binary_Proto_WhenAll             | 30000        | 158.458 ms |  5.6327 ms | 16.1611 ms | 154.716 ms |  2500.0000 | 1500.0000 | 1000.0000 | 17113.17 KB |
| Confluent_Proto_WhenAll          | 30000        | 192.746 ms |  4.7317 ms | 13.5761 ms | 190.687 ms |  6000.0000 | 2000.0000 | 1000.0000 |  39656.1 KB |
| Binary_SpanJson_MessageBatch     | 30000        | 293.452 ms |  9.3522 ms | 26.9831 ms | 288.169 ms |  1000.0000 |         - |         - |  8439.43 KB |
| Binary_Json_Buffer_MessageBatch  | 30000        | 322.736 ms | 10.3859 ms | 29.9658 ms | 317.383 ms |  3000.0000 |         - |         - | 21096.23 KB |
| Confluent_Json_WhenAll           | 30000        | 431.281 ms | 10.8341 ms | 31.2590 ms | 436.511 ms | 10000.0000 | 3500.0000 | 1000.0000 | 57243.85 KB |
