use kvs::sled::SledKvsEngine;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::path::Path;

use criterion::{criterion_group, criterion_main, Criterion};
use kvs::store::KvStore;
use kvs::KvsEngine;

fn gen_keys_values(num: usize, size: usize) -> Vec<(String, String)> {
    let mut kvs: Vec<(String, String)> = Vec::with_capacity(num);
    for _i in 0..num {
        let key_length = thread_rng().gen_range(0..size);
        let key = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(key_length)
            .map(char::from)
            .collect();
        let val_length = rand::thread_rng().gen_range(0..size);
        let val = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(val_length)
            .map(char::from)
            .collect();
        kvs.push((key, val));
    }
    kvs
}

fn bench_write(c: &mut Criterion) {
    let kv_store: KvStore<String, String> = KvStore::open(Path::new("./benches/kvstore")).unwrap();
    let sled_store: SledKvsEngine = SledKvsEngine::new(Path::new("./benches/sledstore")).unwrap();

    let mut group = c.benchmark_group("write");
    group.sample_size(10);
    group.bench_function("kvs_store", |b| {
        let mut kv_vec = gen_keys_values(100, 1000);
        b.iter(|| {
            let (key, val) = kv_vec
                .pop()
                .unwrap_or(("key".to_string(), "value".to_string()));
            kv_store.set(key, val).expect("error while writing values");
        })
    });
    group.bench_function("sled_store", |b| {
        let mut sled_vec = gen_keys_values(100, 1000);
        b.iter(|| {
            let (key, val) = sled_vec
                .pop()
                .unwrap_or(("key".to_string(), "value".to_string()));
            sled_store
                .set(key, val)
                .expect("error while writing values");
        })
    });
    group.finish();
}

// fn kvs_write(c: &mut Criterion) {
//     let mut kv_store: KvStore<String, String> = KvStore::open(Path::new("./benches")).unwrap();
//     let mut group = c.benchmark_group("kvs_write");

//     group.sample_size(2);
//     for kv in ks_vs {
//         group.bench_with_input(BenchmarkId::from_parameter(format!("{},{}", kv.0, kv.1)), &kv, |b, kv| {
//           b.iter(|| {
//               kv_store
//                   .set(kv.0.clone(), kv.1.clone())
//                   .expect("error while writing values");
//           })
//         });
//     }
//     group.finish();
// }

// fn sled_write(c: &mut Criterion) {
//     let mut sled_store: SledKvsEngine = SledKvsEngine::new(Path::new("./benches")).unwrap();
//     let ks_vs = gen_keys_values(100, 1000000);
//     let mut group = c.benchmark_group("kvs_write");
//     for kv in ks_vs {
//         group.bench_with_input(BenchmarkId::from_parameter(format!("{},{}", kv.0, kv.1)), &kv, |b, kv| {
//           b.iter(|| {
//               sled_store
//                   .set(kv.0.clone(), kv.1.clone())
//                   .expect("error while writing values");
//           })
//         });
//     }
//     group.finish();
// }

criterion_group!(benches, bench_write);
criterion_main!(benches);
