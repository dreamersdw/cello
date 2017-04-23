use std::borrow::Cow;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Error, Seek, SeekFrom};
use std::iter::Iterator;
use std::mem;
use std::fmt;
use std::error;
use std::slice;
use std::time;

#[derive(Debug, Clone, Copy)]
pub struct DataPoint {
    pub time: u64,
    pub value: f64,
}

impl fmt::Display for DataPoint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug)]
pub enum Aggregation {
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
pub struct ArchiveSpec {
    pub secs_per_point: u32,
    pub num_of_points: u32,
}

#[derive(Debug)]
pub struct ArchiveInfo {
    secs_per_point: u32,
    num_of_points: u32,
    offset: usize,
}

struct Header {
    meta: Meta,
    archives: Vec<ArchiveInfo>,
}

#[derive(Debug)]
enum RRDError {
    Io(Error),
    InvalidDataPoint(DataPoint),
}

impl fmt::Display for RRDError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            RRDError::Io(ref err) => write!(f, "IO error: {}", err),
            RRDError::InvalidDataPoint(ref err) => write!(f, "Invalid datapoint: {}", err),
        }
    }
}

impl error::Error for RRDError {
    fn description(&self) -> &str {
        match *self {
            RRDError::Io(ref err) => err.description(),
            RRDError::InvalidDataPoint(_) => "Invalid datapoint",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            RRDError::Io(ref err) => Some(err),
            RRDError::InvalidDataPoint(_) => None,
        }
    }
}

impl From<Error> for RRDError {
    fn from(err: Error) -> RRDError {
        RRDError::Io(err)
    }
}


pub struct RRD<'a> {
    path: Cow<'a, str>,
}

impl<'a> RRD<'a> {
    pub fn new<S>(path: S) -> RRD<'a>
        where S: Into<Cow<'a, str>>
    {
        RRD { path: path.into() }
    }

    pub fn init_file(self, archives: &[ArchiveSpec], method: Aggregation) -> Result<(), RRDError> {
        let mut fd = File::create(self.path.as_ref())?;

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
        fd.flush().map_err(|e| RRDError::Io(e))
    }

    fn read_header(&self) -> Result<Header, RRDError> {
        let mut fd = File::open(self.path.as_ref())?;
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

    fn add_point(&mut self, dp: DataPoint) -> Result<(), RRDError> {
        let now = unix_time();
        if dp.time > now {
            return Err(RRDError::InvalidDataPoint(dp));
        }

        let header = self.read_header()?;
        let delta = now - dp.time;
        let mut related_archives = header.archives
            .iter()
            .filter(|e| e.secs_per_point * e.num_of_points > delta as u32);

        let mut high = related_archives.next();
        if high.is_none() {
            return Ok(());
        }

        let archive = high.unwrap();

        let mut fd = OpenOptions::new().write(true)
            .read(true)
            .create(false)
            .open(self.path.as_ref())?;

        fd.seek(SeekFrom::Start(archive.offset as u64))?;
        let last: DataPoint = unsafe { from_bytes(&mut fd) };

        let current = DataPoint {
            time: dp.time - (dp.time % archive.secs_per_point as u64),
            value: dp.value,
        };

        // this is the fist datapoint
        if last.time == 0 {
            let bytes = unsafe { as_bytes(&current) };

            fd.seek(SeekFrom::Start(archive.offset as u64))?;
            fd.write(bytes)?;
        } else {
            let skiped_num = (current.time - last.time) / archive.secs_per_point as u64;
            let skiped_offset = skiped_num * mem::size_of::<DataPoint>() as u64;
            let new_offset = archive.offset as u64 +
                             skiped_offset as u64 %
                             (archive.num_of_points as u64 * mem::size_of::<DataPoint>() as u64);
            let bytes = unsafe { as_bytes(&current) };

            fd.seek(SeekFrom::Start(new_offset as u64))?;
            fd.write(bytes)?;
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
