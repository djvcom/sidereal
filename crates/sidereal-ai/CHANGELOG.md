# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/djvcom/sidereal/releases/tag/sidereal-ai-v0.1.0) - 2026-07-25

### Added

- *(sidereal-ai)* allow unauthenticated Sidereal connections
- *(sidereal-ai)* answer natural-language questions over HTTP
- *(sidereal-ai)* implement Device Code Flow authentication

### Fixed

- *(sidereal-ai)* ground the agent in the current time and SQL dialect
- *(sidereal-ai)* match environment overrides case-insensitively
- *(sidereal-ai)* replace Device Code Flow with Authorization Code + PKCE
- *(sidereal-ai)* use XDG config path on all platforms

### Other

- convert to Cargo workspace layout
