# fileparser

[![Go Reference](https://pkg.go.dev/badge/github.com/nao1215/fileparser.svg)](https://pkg.go.dev/github.com/nao1215/fileparser)
[![Go Report Card](https://goreportcard.com/badge/github.com/nao1215/fileparser)](https://goreportcard.com/report/github.com/nao1215/fileparser)
[![MultiPlatformUnitTest](https://github.com/nao1215/fileparser/actions/workflows/unit_test.yml/badge.svg)](https://github.com/nao1215/fileparser/actions/workflows/unit_test.yml)
![Coverage](https://raw.githubusercontent.com/nao1215/octocovs-central-repo/main/badges/nao1215/fileparser/coverage.svg)

`fileparser` is a Go library for parsing various tabular data formats. It provides a unified interface for reading CSV, TSV, LTSV, Parquet, and XLSX files, with optional compression support.

This package is designed to be used by [filesql](https://github.com/nao1215/filesql), [fileprep](https://github.com/nao1215/fileprep), and [fileframe](https://github.com/nao1215/fileframe).

- fileprep: struct-tag preprocessing and validation for CSV/TSV/LTSV, Parquet, Excel.
- filesql: sql driver for CSV, TSV, LTSV, Parquet, Excel with compression support.
- fileframe: DataFrame API for CSV/TSV/LTSV, Parquet, Excel.

## Features

- Multiple formats: CSV, TSV, LTSV, Parquet, XLSX
- Compression support: gzip, bzip2, xz, zstd, zlib, snappy, s2, lz4
- Type inference: Automatically detects column types (TEXT, INTEGER, REAL, DATETIME)
- File type detection: Detects file format from path extension
- Pure Go: No CGO required for any compression format

## Installation

```bash
go get github.com/nao1215/fileparser
```

## Usage

### Parsing CSV

```go
csvData := `name,age,score
Alice,30,85.5
Bob,25,92.0
Charlie,35,78.5`

result, err := fileparser.Parse(strings.NewReader(csvData), fileparser.CSV)
if err != nil {
    log.Fatal(err)
}

fmt.Println("Headers:", result.Headers)
fmt.Println("Records:", len(result.Records))
fmt.Println("First row:", result.Records[0])
```

Output:

```text
Headers: [name age score]
Records: 3
First row: [Alice 30 85.5]
```

### Parsing TSV

```go
tsvData := `id	product	price
1	Laptop	999.99
2	Mouse	29.99
3	Keyboard	79.99`

result, err := fileparser.Parse(strings.NewReader(tsvData), fileparser.TSV)
if err != nil {
    log.Fatal(err)
}

fmt.Println("Headers:", result.Headers)
fmt.Println("Records:", len(result.Records))
```

Output:

```text
Headers: [id product price]
Records: 3
```

### Parsing LTSV

```go
ltsvData := `host:192.168.1.1	method:GET	path:/index.html
host:192.168.1.2	method:POST	path:/api/users`

result, err := fileparser.Parse(strings.NewReader(ltsvData), fileparser.LTSV)
if err != nil {
    log.Fatal(err)
}

fmt.Println("Headers:", result.Headers)
fmt.Println("First row:", result.Records[0])
```

Output:

```text
Headers: [host method path]
First row: [192.168.1.1 GET /index.html]
```

### Auto-detect File Type

```go
paths := []string{
    "data.csv",
    "data.csv.gz",
    "report.xlsx",
    "logs.ltsv.zst",
    "analytics.parquet",
}

for _, path := range paths {
    ft := fileparser.DetectFileType(path)
    fmt.Printf("%s -> %s\n", path, ft)
}
```

Output:

```text
data.csv -> CSV
data.csv.gz -> CSV (gzip)
report.xlsx -> XLSX
logs.ltsv.zst -> LTSV (zstd)
analytics.parquet -> Parquet
```

### Check Compression

```go
types := []fileparser.FileType{
    fileparser.CSV,
    fileparser.CSVGZ,
    fileparser.Parquet,
    fileparser.ParquetZSTD,
}

for _, ft := range types {
    fmt.Printf("%s compressed: %v\n", ft, fileparser.IsCompressed(ft))
}
```

Output:

```text
CSV compressed: false
CSV (gzip) compressed: true
Parquet compressed: false
Parquet (zstd) compressed: true
```

### Get Base File Type

```go
types := []fileparser.FileType{
    fileparser.CSV,
    fileparser.CSVGZ,
    fileparser.TSVBZ2,
    fileparser.ParquetZSTD,
}

for _, ft := range types {
    base := fileparser.BaseFileType(ft)
    fmt.Printf("%s -> %s\n", ft, base)
}
```

Output:

```text
CSV -> CSV
CSV (gzip) -> CSV
TSV (bzip2) -> TSV
Parquet (zstd) -> Parquet
```

### Convert String Values to Typed Values

```go
// Integer column
intVal := fileparser.ParseValue("42", fileparser.TypeInteger)
fmt.Printf("Integer: %v (%T)\n", intVal, intVal)

// Real column
realVal := fileparser.ParseValue("3.14", fileparser.TypeReal)
fmt.Printf("Real: %v (%T)\n", realVal, realVal)

// Text column
textVal := fileparser.ParseValue("hello", fileparser.TypeText)
fmt.Printf("Text: %v (%T)\n", textVal, textVal)

// Empty value returns nil
nilVal := fileparser.ParseValue("", fileparser.TypeInteger)
fmt.Printf("Empty: %v\n", nilVal)
```

Output:

```text
Integer: 42 (int64)
Real: 3.14 (float64)
Text: hello (string)
Empty: <nil>
```

### Automatic Column Type Inference

```go
csvData := `id,name,score,date
1,Alice,85.5,2024-01-15
2,Bob,92.0,2024-01-16
3,Charlie,78.5,2024-01-17`

result, err := fileparser.Parse(strings.NewReader(csvData), fileparser.CSV)
if err != nil {
    log.Fatal(err)
}

for i, header := range result.Headers {
    fmt.Printf("%s: %s\n", header, result.ColumnTypes[i])
}
```

Output:

```text
id: INTEGER
name: TEXT
score: REAL
date: DATETIME
```

## Supported File Types

| Format  | Extension | Compressed Variants |
|---------|-----------|---------------------|
| CSV     | `.csv`    | `.csv.gz`, `.csv.bz2`, `.csv.xz`, `.csv.zst`, `.csv.z`, `.csv.snappy`, `.csv.s2`, `.csv.lz4` |
| TSV     | `.tsv`    | `.tsv.gz`, `.tsv.bz2`, `.tsv.xz`, `.tsv.zst`, `.tsv.z`, `.tsv.snappy`, `.tsv.s2`, `.tsv.lz4` |
| LTSV    | `.ltsv`   | `.ltsv.gz`, `.ltsv.bz2`, `.ltsv.xz`, `.ltsv.zst`, `.ltsv.z`, `.ltsv.snappy`, `.ltsv.s2`, `.ltsv.lz4` |
| Parquet | `.parquet`| `.parquet.gz`, `.parquet.bz2`, `.parquet.xz`, `.parquet.zst`, `.parquet.z`, `.parquet.snappy`, `.parquet.s2`, `.parquet.lz4` |
| XLSX    | `.xlsx`   | `.xlsx.gz`, `.xlsx.bz2`, `.xlsx.xz`, `.xlsx.zst`, `.xlsx.z`, `.xlsx.snappy`, `.xlsx.s2`, `.xlsx.lz4` |

## Compression Formats

| Format | Extension | Library |
|--------|-----------|---------|
| gzip   | `.gz`     | `compress/gzip` (standard library) |
| bzip2  | `.bz2`    | `compress/bzip2` (standard library) |
| xz     | `.xz`     | `github.com/ulikunitz/xz` |
| zstd   | `.zst`    | `github.com/klauspost/compress/zstd` |
| zlib   | `.z`      | `compress/zlib` (standard library) |
| Snappy | `.snappy` | `github.com/klauspost/compress/snappy` |
| S2     | `.s2`     | `github.com/klauspost/compress/s2` |
| LZ4    | `.lz4`    | `github.com/pierrec/lz4/v4` |

## Column Types

The parser automatically infers column types based on the data:

| Type | Description |
|------|-------------|
| `TypeText` | String/text data |
| `TypeInteger` | Integer numbers |
| `TypeReal` | Floating-point numbers |
| `TypeDatetime` | Date and time values |

## License

MIT License. See [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
