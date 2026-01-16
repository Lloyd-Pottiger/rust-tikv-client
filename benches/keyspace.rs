use criterion::black_box;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use tikv_client::request::EncodeKeyspace;
use tikv_client::request::KeyMode;
use tikv_client::request::Keyspace;
use tikv_client::request::TruncateKeyspace;
use tikv_client::Key;

fn bench_keyspace_encode_truncate(c: &mut Criterion) {
    let keyspace = Keyspace::Enable { keyspace_id: 42 };
    let key = Key::from(vec![b'k'; 32]);

    let mut group = c.benchmark_group("keyspace");
    group.bench_function("encode_raw_32b", |b| {
        b.iter(|| {
            let encoded = black_box(key.clone()).encode_keyspace(keyspace, KeyMode::Raw);
            black_box(encoded);
        })
    });

    group.bench_function("truncate_raw_32b", |b| {
        let encoded = key.clone().encode_keyspace(keyspace, KeyMode::Raw);
        b.iter(|| {
            let truncated = black_box(encoded.clone()).truncate_keyspace(keyspace);
            black_box(truncated);
        })
    });

    group.finish();
}

criterion_group!(benches, bench_keyspace_encode_truncate);
criterion_main!(benches);
