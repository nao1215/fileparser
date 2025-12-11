# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2024-12-11

### Added

- Initial release of fileparser
- Support for multiple tabular data formats:
  - CSV (Comma-Separated Values)
  - TSV (Tab-Separated Values)
  - LTSV (Labeled Tab-Separated Values)
  - Parquet (Apache Parquet)
  - XLSX (Microsoft Excel)
- Compression support for all formats:
  - gzip (`.gz`)
  - bzip2 (`.bz2`)
  - xz (`.xz`)
  - zstd (`.zst`)
- Automatic file type detection from path extension (`DetectFileType`)
- Compression detection (`IsCompressed`)
- Base file type extraction (`BaseFileType`)
- Automatic column type inference:
  - `TypeText` for string data
  - `TypeInteger` for integer numbers
  - `TypeReal` for floating-point numbers
  - `TypeDatetime` for date and time values
- Type-safe value parsing (`ParseValue`)
- `TableData` structure containing:
  - `Headers` - column names
  - `Records` - data rows as string slices
  - `ColumnTypes` - inferred types for each column

[0.1.0]: https://github.com/nao1215/fileparser/releases/tag/v0.1.0
