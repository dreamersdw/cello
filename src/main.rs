use std::borrow::Cow;
use std::clone::Clone;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Error, Seek, SeekFrom};
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

#[derive(Debug)]
enum Aggregation {
    Avg,
    Max,
    Min,
    Sum,
    Last,
}

#[derive(Debug)]
struct Meta {
    magic_number: [u8; 5],
    aggregation_method: Aggregation,
    max_rentention: u32,
    num_of_archives: u32,
}

fn unix_time() -> u64 {
    match time::SystemTime::now().duration_since(time::UNIX_EPOCH) {
        Ok(d) => d.as_secs(),
        Err(_) => 0,
    }
}

#[derive(Debug)]
struct ArchiveSpec {
    secs_per_point: u32,
    num_of_points: u32,
}

#[derive(Debug)]
struct ArchiveInfo {
    secs_per_point: u32,
    num_of_points: u32,
    offset: usize,
}

struct Header {
    meta: Meta,
    archives: Vec<ArchiveInfo>,
}

struct RRD {
    path: &'static str,
}

impl RRD {
    fn new(path: &'static str) -> RRD {
        RRD { path: path }
    }

    fn init_file(self, archives: &[ArchiveSpec], method: Aggregation) -> Result<(), Error> {
        let mut fd = File::create(self.path)?;

        let mut total_size: u64 = 0;
        let meta_size: usize;

        // write header meta
        let oldest = archives.iter().map(|a| a.secs_per_point * a.num_of_points).max().unwrap();
        let meta = Meta {
            magic_number: *b"cello",
            aggregation_method: method,
            max_rentention: oldest,
            num_of_archives: archives.len() as u32,
        };

        let meta_bytes = unsafe { as_bytes(&meta) };
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
            let archive_bytes = unsafe { as_bytes(&archive_info) };
            fd.write(archive_bytes)?;
            total_size += archive_bytes.len() as u64;

            offset += spec.num_of_points as usize * mem::size_of::<DataPoint>();
        }

        // write data point placeholders
        let point_size = archives.iter()
            .map(|x| x.num_of_points * mem::size_of::<DataPoint>() as u32)
            .fold(0, |sum, i| sum + i);

        total_size += point_size as u64;
        fd.set_len(total_size).unwrap();
        fd.flush()
    }

    fn read_header(&self) -> Result<Header, Error> {
        let mut fd = File::open(self.path)?;
        let meta: Meta = unsafe { from_bytes(&mut fd) };
        let mut archives: Vec<ArchiveInfo> = Vec::with_capacity(meta.num_of_archives as usize);

        for _ in 0..meta.num_of_archives {
            let archive: ArchiveInfo = unsafe { from_bytes(&mut fd) };
            archives.push(archive);
        }

        Ok(Header {
            meta: meta,
            archives: archives,
        })
    }

    fn add_point(&mut self, dp: DataPoint) -> Result<(), Error> {
        let mut fd = OpenOptions::new().write(true)
            .read(true)
            .create(false)
            .open(self.path)?;

        let header = self.read_header()?;

        let meta_size = mem::size_of::<Meta>();

        for (i, archive) in header.archives.iter().enumerate() {
            fd.seek(SeekFrom::Start(archive.offset as u64))?;
            let last: DataPoint = unsafe { from_bytes(&mut fd) };
            let current = DataPoint {
                time: dp.time - (dp.time % archive.secs_per_point as u64),
                value: dp.value,
            };

            // this is the fist datapoint
            if last.time == 0 {
                fd.seek(SeekFrom::Start(archive.offset as u64))?;
                let bytes = unsafe { as_bytes(&current) };
                fd.write(bytes)?;
            } else {
                let skiped_num = (current.time - last.time) / archive.secs_per_point as u64;
                let skiped_offset = skiped_num * mem::size_of::<DataPoint>() as u64;
                let new_offset = archive.offset as u64 +
                                 skiped_offset as u64 %
                                 (archive.num_of_points as u64 *
                                  mem::size_of::<DataPoint>() as u64);
                fd.seek(SeekFrom::Start(new_offset as u64))?;
                let bytes = unsafe { as_bytes(&current) };
                fd.write(bytes)?;

                let archive_offset = meta_size as u64 +
                                     i as u64 * mem::size_of::<ArchiveInfo>() as u64;
                fd.seek(SeekFrom::Start(archive_offset))?;
                let bytes = unsafe {
                    as_bytes(&ArchiveInfo {
                        num_of_points: archive.num_of_points,
                        secs_per_point: archive.secs_per_point,
                        offset: new_offset as usize,
                    })
                };
                fd.write(bytes)?;
            }
        }

        fd.flush()?;
        Ok(())
    }
}

unsafe fn as_bytes<'a, T>(val: &T) -> &'a [u8] {
    slice::from_raw_parts(val as *const T as *const u8, mem::size_of::<T>())
}

unsafe fn from_bytes<T>(reader: &mut File) -> T {
    let mut val: T = mem::uninitialized();
    let t_size = mem::size_of::<T>();
    let t_slice = slice::from_raw_parts_mut(&mut val as *mut T as *mut u8, t_size);

    reader.read_exact(t_slice).unwrap();
    val
}

fn main() {}

#[test]
fn test_read_write_file() {
    let rrd = RRD::new("/tmp/example.rrd");
    let result = rrd.init_file(&[ArchiveSpec {
                                     secs_per_point: 60,
                                     num_of_points: 86400 * 1 / 60,
                                 }],
                               Aggregation::Avg);
    assert!(result.is_ok());

    let rrd = RRD::new("/tmp/example.rrd");
    let result = rrd.read_header();
    assert!(result.is_ok());
    let header = result.unwrap();
    assert_eq!(header.meta.num_of_archives, 1);
    assert_eq!(header.archives.len(), 1);


    let mut rrd = RRD::new("/tmp/example.rrd");
    for i in 0..60 * 48 {
        let result = rrd.add_point(DataPoint {
            time: unix_time() + i * 60,
            value: 12.3,
        });
        assert!(result.is_ok());
    }
}
