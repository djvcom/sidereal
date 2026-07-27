# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/djvcom/sidereal/compare/sidereal-v0.1.0...sidereal-v0.2.0) - 2026-07-27

### Added

- *(buffer)* add write-ahead log for crash-safe ingestion

### Other

- release v0.1.0

## [0.1.0](https://github.com/djvcom/sidereal/releases/tag/sidereal-v0.1.0) - 2026-07-25

### Added

- *(sidereal)* delete telemetry past a retention window
- *(sidereal)* expose information_schema in the query engine
- *(auth)* add OIDC JWT validation for the query API

### Fixed

- *(sidereal)* partition flushed batches by hour
- *(sidereal)* read timeline buckets at any timestamp precision
- *(sidereal)* cast timeline timestamps before division
- *(sidereal)* return DataFusion messages from SQL query errors
- *(sidereal)* read count aggregates as signed integers
- *(sidereal)* broadcast scalar arguments in error_fingerprint
- *(sidereal)* construct ingesters with storage schemas

### Other

- *(sidereal)* pin converted metrics to the storage schema
- *(sidereal)* cover the errors module end to end
- convert to Cargo workspace layout
