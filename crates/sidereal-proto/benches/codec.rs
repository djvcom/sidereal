//! Benchmarks for the protocol codec.
//!
//! Run with: cargo bench -p sidereal-proto

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use sidereal_proto::codec::{Codec, MessageType};
use sidereal_proto::{
    ControlMessage, Envelope, FunctionMessage, InvokeRequest, StateMessage, StateRequest,
    StateResponse,
};

fn create_invoke_request(payload_size: usize) -> Envelope<FunctionMessage> {
    let request = InvokeRequest::new("test-function", vec![0u8; payload_size]);
    Envelope::new(FunctionMessage::Invoke(request))
}

fn create_state_request() -> Envelope<StateMessage> {
    Envelope::new(StateMessage::Request(StateRequest::KvGet {
        key: "test-key".to_string(),
    }))
}

fn create_state_response(payload_size: usize) -> Envelope<StateMessage> {
    Envelope::new(StateMessage::Response(StateResponse::KvValue(Some(
        vec![0u8; payload_size],
    ))))
}

fn create_control_ping() -> Envelope<ControlMessage> {
    Envelope::new(ControlMessage::Ping)
}

fn bench_encode_function(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_function");

    for size in [64, 1024, 8192, 65536].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let mut codec = Codec::with_capacity(size + 1024);
            let envelope = create_invoke_request(size);

            b.iter(|| {
                let result = codec.encode(black_box(&envelope), MessageType::Function);
                black_box(result.unwrap().len())
            });
        });
    }

    group.finish();
}

fn bench_decode_function(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode_function");

    for size in [64, 1024, 8192, 65536].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let mut codec = Codec::with_capacity(size + 1024);
            let envelope = create_invoke_request(size);
            let bytes = codec
                .encode(&envelope, MessageType::Function)
                .unwrap()
                .to_vec();

            // Skip frame header (8 bytes)
            let payload = &bytes[8..];

            b.iter(|| {
                let result: Envelope<FunctionMessage> = Codec::decode(black_box(payload)).unwrap();
                black_box(result)
            });
        });
    }

    group.finish();
}

fn bench_encode_state(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_state");

    // State request (small)
    group.bench_function("request", |b| {
        let mut codec = Codec::with_capacity(1024);
        let envelope = create_state_request();

        b.iter(|| {
            let result = codec.encode(black_box(&envelope), MessageType::State);
            black_box(result.unwrap().len())
        });
    });

    // State response with various payload sizes
    for size in [64, 1024, 8192].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(
            BenchmarkId::new("response", size),
            size,
            |b, &size| {
                let mut codec = Codec::with_capacity(size + 1024);
                let envelope = create_state_response(size);

                b.iter(|| {
                    let result = codec.encode(black_box(&envelope), MessageType::State);
                    black_box(result.unwrap().len())
                });
            },
        );
    }

    group.finish();
}

fn bench_decode_state(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode_state");

    // State request (small)
    group.bench_function("request", |b| {
        let mut codec = Codec::with_capacity(1024);
        let envelope = create_state_request();
        let bytes = codec.encode(&envelope, MessageType::State).unwrap().to_vec();
        let payload = &bytes[8..];

        b.iter(|| {
            let result: Envelope<StateMessage> = Codec::decode(black_box(payload)).unwrap();
            black_box(result)
        });
    });

    // State response with various payload sizes
    for size in [64, 1024, 8192].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(
            BenchmarkId::new("response", size),
            size,
            |b, &size| {
                let mut codec = Codec::with_capacity(size + 1024);
                let envelope = create_state_response(size);
                let bytes = codec.encode(&envelope, MessageType::State).unwrap().to_vec();
                let payload = &bytes[8..];

                b.iter(|| {
                    let result: Envelope<StateMessage> = Codec::decode(black_box(payload)).unwrap();
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

fn bench_encode_control(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_control");

    group.bench_function("ping", |b| {
        let mut codec = Codec::with_capacity(256);
        let envelope = create_control_ping();

        b.iter(|| {
            let result = codec.encode(black_box(&envelope), MessageType::Control);
            black_box(result.unwrap().len())
        });
    });

    group.finish();
}

fn bench_decode_control(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode_control");

    group.bench_function("ping", |b| {
        let mut codec = Codec::with_capacity(256);
        let envelope = create_control_ping();
        let bytes = codec
            .encode(&envelope, MessageType::Control)
            .unwrap()
            .to_vec();
        let payload = &bytes[8..];

        b.iter(|| {
            let result: Envelope<ControlMessage> = Codec::decode(black_box(payload)).unwrap();
            black_box(result)
        });
    });

    group.finish();
}

fn bench_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("roundtrip");

    for size in [64, 1024, 8192].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let mut codec = Codec::with_capacity(size + 1024);
            let envelope = create_invoke_request(size);

            b.iter(|| {
                let bytes = codec
                    .encode(black_box(&envelope), MessageType::Function)
                    .unwrap()
                    .to_vec();
                let payload = &bytes[8..];
                let decoded: Envelope<FunctionMessage> = Codec::decode(black_box(payload)).unwrap();
                black_box(decoded)
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_encode_function,
    bench_decode_function,
    bench_encode_state,
    bench_decode_state,
    bench_encode_control,
    bench_decode_control,
    bench_roundtrip,
);

criterion_main!(benches);
