#![feature(bench_black_box)]
use std::{fs::File, hint::black_box, mem::transmute, time::Instant};

use rand::Rng;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

fn base() {}

#[derive(Serialize, Deserialize)]
struct FnPtrMapper {
    f: usize,
}

impl FnPtrMapper {
    pub fn new(f: fn(usize) -> usize) -> Self {
        Self {
            f: f as *const () as usize - (base as *const () as usize),
        }
    }
}

impl MapRdd for FnPtrMapper {
    fn run(&self, v: Vec<usize>) -> Vec<usize> {
        // shouldn't matter for perf
        let f: fn(usize) -> usize =
            unsafe { transmute(((base as *const () as usize) + self.f) as *const ()) };
        v.into_iter().map(f).collect()
    }
}

trait Mapper {
    type Item;
    type Res;
    fn work(&self, item: Self::Item) -> Self::Res;
}

#[derive(Serialize, Deserialize)]
struct Mul2Mapper();
impl Mapper for Mul2Mapper {
    type Item = usize;
    type Res = usize;
    #[inline(always)]
    fn work(&self, item: Self::Item) -> Self::Res {
        item * 2
    }
}

#[derive(Serialize, Deserialize)]
struct RetZeroMapper();
impl Mapper for RetZeroMapper {
    type Item = usize;
    type Res = usize;
    #[inline(always)]
    fn work(&self, _item: Self::Item) -> Self::Res {
        0
    }
}

#[derive(Serialize, Deserialize)]
struct Pow2Mapper();
impl Mapper for Pow2Mapper {
    type Item = usize;
    type Res = usize;
    #[inline(always)]
    fn work(&self, item: Self::Item) -> Self::Res {
        item.pow(2) / 100
    }
}

#[derive(Serialize, Deserialize)]
struct GenericsMapper<T> {
    mapper: T,
}

impl<T> GenericsMapper<T>
where
    T: Mapper<Item = usize, Res = usize>,
{
    pub fn new(mapper: T) -> Self {
        Self { mapper }
    }
}

impl<T> MapRdd for GenericsMapper<T>
where
    T: Mapper<Item = usize, Res = usize> + Serialize + DeserializeOwned,
{
    fn run(&self, v: Vec<usize>) -> Vec<usize> {
        v.into_iter().map(|x| self.mapper.work(x)).collect()
    }
}

trait MapRdd: serde_traitobject::Serialize + serde_traitobject::Deserialize {
    fn run(&self, v: Vec<usize>) -> Vec<usize>;
}

// just for serialization, ignore this struct
#[derive(Serialize, Deserialize)]
struct RddContainer {
    #[serde(with = "serde_traitobject")]
    map_rdd: Box<dyn MapRdd>,
}

fn main() {
    // create few different types of mappers otherwise compiler will optimize it :)
    let mul_2_fn: RddContainer = RddContainer {
        map_rdd: Box::new(FnPtrMapper::new(|x| x * 2)),
    };
    let pow_2_fn: RddContainer = RddContainer {
        map_rdd: Box::new(FnPtrMapper::new(|x| x.pow(2) / 100)),
    };
    let ret_0_fn: RddContainer = RddContainer {
        map_rdd: Box::new(FnPtrMapper::new(|_x| 0)),
    };
    let mul_2_gen: RddContainer = RddContainer {
        map_rdd: Box::new(GenericsMapper::new(Mul2Mapper())),
    };
    let pow_2_gen: RddContainer = RddContainer {
        map_rdd: Box::new(GenericsMapper::new(Pow2Mapper())),
    };
    let ret_0_gen: RddContainer = RddContainer {
        map_rdd: Box::new(GenericsMapper::new(RetZeroMapper())),
    };

    let mut r = rand::thread_rng();
    if r.gen() {
        let (a, b): (RddContainer, RddContainer) = vec![(mul_2_fn, mul_2_gen), (pow_2_fn, pow_2_gen), (ret_0_fn, ret_0_gen)].remove(r.gen_range(0..3));

        serde_json::to_writer_pretty(File::create("./a").unwrap(), &a).unwrap();
        serde_json::to_writer_pretty(File::create("./b").unwrap(), &b).unwrap();
    }

    let a: RddContainer = serde_json::from_reader(File::open("./a").unwrap()).unwrap();
    let b: RddContainer = serde_json::from_reader(File::open("./b").unwrap()).unwrap();

    const N: usize = 100;
    const SZ: usize = 0x1000 * 0x1000;
    let mut sm: u128 = 0;
    let mut r = 0;
    for _ in 0..N {
        let v = black_box(vec![15; SZ]);
        let start = Instant::now();
        let v = black_box(a.map_rdd.run(v));
        let duration = start.elapsed();
        r = v[0];
        sm += duration.as_nanos();
    }
    let avg_fn = (sm as f64) / (N as f64);
    println!("fn  way: avg_ns: {avg_fn}; r: {r}",);

    let mut sm: u128 = 0;
    let mut r = 0;
    for _ in 0..N {
        let v = black_box(vec![15; SZ]);
        let start = Instant::now();
        let v = black_box(b.map_rdd.run(v));
        let duration = start.elapsed();
        r = v[0];
        sm += duration.as_nanos();
    }
    let avg_gen = (sm as f64) / (N as f64);
    println!("gen way: avg_ns: {avg_gen}; r: {r}");
    println!("speedup coeff: {}", avg_fn / avg_gen);
}
