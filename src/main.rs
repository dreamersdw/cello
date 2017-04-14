use std::borrow::Cow;
use std::clone::Clone;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Write, Error, Seek, SeekFrom};
use std::iter::Iterator;
use std::mem;
use std::slice;
use std::time;

#[derive(Debug, Clone, Copy)]
struct DataPoint {
    time: u64,
    value: f64,
}

struct TS<'a> {
    key: Cow<'a, str>,
    points: Vec<DataPoint>,
}

struct TSMap<'a> {
    map: HashMap<Cow<'a, str>, TS<'a>>,
}

impl<'a> TSMap<'a> {
    fn new() -> TSMap<'a> {
        TSMap { map: HashMap::new() }
    }

    fn add_point<S>(&mut self, key: S, dp: DataPoint)
        where S: Into<Cow<'a, str>>
    {
        let ckey = key.into();
        let mut ts = self.map.entry(ckey.clone()).or_insert(TS {
            key: ckey.clone(),
            points: vec![],
        });

        ts.add_point(dp);
    }

    fn query<S>(&self, key: S, start: u64, end: u64) -> Option<Vec<&DataPoint>>
        where S: Into<Cow<'a, str>>
    {
        self.map
            .get(&key.into())
            .map(|ts| {
                ts.points
                    .iter()
                    .take_while(|x| x.time >= start && x.time <= end)
                    .collect()
            })
    }
}

impl<'a> TS<'a> {
    fn add_point(&mut self, dp: DataPoint) {
        self.points.push(dp);
    }
}

impl<'a> IntoIterator for TS<'a> {
    type Item = DataPoint;
    type IntoIter = TsIntoIterator<'a>;
    fn into_iter(self) -> Self::IntoIter {
        TsIntoIterator {
            ts: self,
            index: 0,
        }
    }
}

struct TsIntoIterator<'a> {
    ts: TS<'a>,
    index: usize,
}

impl<'a> Iterator for TsIntoIterator<'a> {
    type Item = DataPoint;
    fn next(&mut self) -> Option<DataPoint> {
        let result = if self.index < self.ts.points.len() {
            Some(self.ts.points[self.index])
        } else {
            None
        };
        self.index += 1;
        result
    }
}

enum Aggregation {
    Avg,
    Max,
    Min,
    Sum,
    Last,
}

struct Meta {
    magic_number: [u8; 5],
    aggregation_method: Aggregation,
    max_rentention: u32,
}

fn unix_time() -> u64 {
    match time::SystemTime::now().duration_since(time::UNIX_EPOCH) {
        Ok(d) => d.as_secs(),
        Err(_) => 0,
    }
}

struct ArchiveSpec {
    secs_per_point: u32,
    num_of_points: u32,
}

struct ArchiveInfo {
    secs_per_point: u32,
    num_of_points: u32,
    offset: usize,
}

struct RRD {
    path: &'static str,
}

unsafe fn as_bytes<'a, T>(val: T) -> &'a [u8] {
    slice::from_raw_parts(&val as *const T as *const u8, mem::size_of::<T>())
}

impl RRD {
    fn new(path: &'static str) -> RRD {
        RRD { path: path }
    }

    fn init_file(self, archives: &[ArchiveSpec], method: Aggregation) -> Result<(), Error> {
        let mut fd = File::create(self.path)?;

        let mut total_size = 0;
        let meta_size: usize;

        // write header meta
        let oldest = archives.iter().map(|a| a.secs_per_point * a.num_of_points).max().unwrap();
        let meta = Meta {
            magic_number: *b"cello",
            aggregation_method: method,
            max_rentention: oldest,
        };
        let meta_bytes = unsafe { as_bytes(meta) };
        fd.write(meta_bytes)?;
        meta_size = meta_bytes.len();

        // write archive info
        let mut offset = meta_size + archives.len() * mem::size_of::<ArchiveInfo>();
        for spec in archives {
            let archive_info = ArchiveInfo {
                num_of_points: spec.num_of_points,
                secs_per_point: spec.secs_per_point,
                offset: offset,
            };
            let archive_bytes = unsafe { as_bytes(archive_info) };
            fd.write(archive_bytes)?;
            total_size += archive_bytes.len();

            offset += spec.num_of_points as usize * mem::size_of::<DataPoint>();
        }

        // write data point placeholders
        let point_size = archives.iter()
            .map(|x| x.num_of_points * mem::size_of::<DataPoint>() as u32)
            .fold(0, |sum, i| sum + i);

        total_size += point_size as usize;
        fd.seek(SeekFrom::Start(total_size as u64 - 1))?;
        fd.write(&[0x0])?;
        fd.flush()
    }

    #[allow(dead_code)]
    fn add_point(_: DataPoint) -> Result<(), Error> {
        unimplemented!()
    }
}

fn main() {
    let f = RRD::new("example.rrd");

    f.init_file(&[ArchiveSpec {
                         secs_per_point: 60,
                         num_of_points: 86400 * 1 / 60,
                     },
                     ArchiveSpec {
                         secs_per_point: 60 * 5,
                         num_of_points: 86400 * 7 / 300,
                     },
                     ArchiveSpec {
                         secs_per_point: 60 * 30,
                         num_of_points: 86400 * 30 / 1800,
                     },
                     ArchiveSpec {
                         secs_per_point: 60 * 60 * 12,
                         num_of_points: 86400 * 365 / 43200,
                     }],
                   Aggregation::Avg)
        .expect("unable to create file");
}
