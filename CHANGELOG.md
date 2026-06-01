# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.2] - 2026-06-01

### Fixed

- **LTSV duplicate labels are now rejected**: `parseLTSV` previously kept only the last value when a record repeated a label (e.g. `x:1\tx:2`), silently dropping the earlier value. A duplicate label within a record is now rejected with a `duplicate column name` error, matching the CSV/TSV parsers and keeping LTSV imports lossless. A label repeated across separate records (the normal column-per-row case) still parses.

## [0.5.1] - 2026-02-14

### Fixed

- **Fedwire round-trip editing now supports adding new sections**: `applyModifications` previously skipped nil sub-structs entirely, silently dropping any values set via SQL editing for sections absent in the original file. Nil sub-structs are now lazily allocated when the TableData record contains non-empty values.

## [0.5.0] - 2026-02-14

### Added

- **Fedwire file support** (Experimental):
  - New `wire` subpackage for parsing legacy Fedwire message files (`.fed`)
  - Bidirectional conversion between Fedwire files and TableData structures
  - Single flat table with ~326 columns covering all FEDWireMessage fields
  - All columns are `TypeText` (wire format stores everything as strings)
  - Column groups: SenderSupplied, TypeSubType, IMAD, Amount, SenderDI, ReceiverDI, BusinessFunctionCode, financial institutions, parties, FI-to-FI information, advice records, cover payment, remittance, and system fields
  - Round-trip editing: Fedwire → TableData → SQL modifications → Fedwire
  - `ParseReader()` and `WriteToWriter()` for stream-based I/O
  - Nil-safe public API: `ParseReader(nil)` and `WriteToWriter(nil)` return errors instead of panicking
  - Built on [moov-io/wire](https://github.com/moov-io/wire) library
  - Subpackage-only design (same as ACH): `fileparser.Parse()` and `DetectFileType()` do not handle `.fed` files

## [0.4.0] - 2026-02-13

### Added

- **JSON file support** (`.json`):
  - Array root: each element becomes a row with raw JSON in the `data` column
  - Object root: single row with the entire object as raw JSON
  - Nested structures are preserved and queryable via SQLite's `json_extract()`
  - Compression support: `.json.gz`, `.json.bz2`, `.json.xz`, `.json.zst`, `.json.z`, `.json.snappy`, `.json.s2`, `.json.lz4`
- **JSONL (JSON Lines) file support** (`.jsonl`):
  - Each non-empty line becomes a row with raw JSON in the `data` column
  - Empty lines are silently skipped
  - Compression support: `.jsonl.gz`, `.jsonl.bz2`, `.jsonl.xz`, `.jsonl.zst`, `.jsonl.z`, `.jsonl.snappy`, `.jsonl.s2`, `.jsonl.lz4`
- New FileType constants: `JSON`, `JSONL`, `JSONGZ`, `JSONBZ2`, `JSONXZ`, `JSONZSTD`, `JSONZLIB`, `JSONSNAPPY`, `JSONS2`, `JSONLZ4`, `JSONLGZ`, `JSONLBZ2`, `JSONLXZ`, `JSONLZSTD`, `JSONLZLIB`, `JSONLSNAPPY`, `JSONLS2`, `JSONLLZ4`

### Fixed

- Preallocate `records` slices in ACH entry/IAT entry conversion to avoid unnecessary allocations

## [0.3.0] - 2025-12-14

### Added

- **ACH (NACHA) file support** (Experimental):
  - New `ach` subpackage for parsing ACH files following NACHA format
  - Bidirectional conversion between ACH files and TableData structures
  - Support for standard batches and IAT (International ACH Transaction) batches
  - Multiple table types: `file_header`, `batches`, `entries`, `addenda`, `iat_batches`, `iat_entries`, `iat_addenda`
  - Support for all addenda types:
    - Standard: Addenda02, Addenda05, Addenda98, Addenda98Refused, Addenda99, Addenda99Dishonored, Addenda99Contested
    - IAT: Addenda10-18, Addenda98, Addenda99
  - Round-trip editing: ACH → TableData → SQL modifications → ACH
  - `ParseReader()` and `WriteToWriter()` functions for stream-based I/O
  - Built on [moov-io/ach](https://github.com/moov-io/ach) library

## [0.2.0] - 2025-12-11

### Added

- New compression format support (pure Go, no CGO required):
  - zlib (`.z`) - using `compress/zlib` standard library
  - Snappy (`.snappy`) - using `github.com/klauspost/compress/snappy`
  - S2 (`.s2`) - using `github.com/klauspost/compress/s2`
  - LZ4 (`.lz4`) - using `github.com/pierrec/lz4/v4`
- New FileType constants for all compression combinations:
  - CSV: `CSVZLIB`, `CSVSNAPPY`, `CSVS2`, `CSVLZ4`
  - TSV: `TSVZLIB`, `TSVSNAPPY`, `TSVS2`, `TSVLZ4`
  - LTSV: `LTSVZLIB`, `LTSVSNAPPY`, `LTSVS2`, `LTSVLZ4`
  - Parquet: `ParquetZLIB`, `ParquetSNAPPY`, `ParquetS2`, `ParquetLZ4`
  - XLSX: `XLSXZLIB`, `XLSXSNAPPY`, `XLSXS2`, `XLSXLZ4`

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

[0.5.1]: https://github.com/nao1215/fileparser/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/nao1215/fileparser/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/nao1215/fileparser/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/nao1215/fileparser/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/nao1215/fileparser/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/nao1215/fileparser/releases/tag/v0.1.0
