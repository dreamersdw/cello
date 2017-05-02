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

#[derive(Debug, Copy, Clone)]
pub enum Aggregation {
    Avg,
    Max,
    Min,
    Sum,
    Last,
}

fn aggreate(values: Vec<f64>, method: Aggregation) -> f64 {
    match method {
        Aggregation::Avg => {
            let num = values.len() as f64;
            let sum = values.iter().cloned().fold(0.0, |acc, x| acc + x);
            sum / num
        },
        Aggregation::Max => {
            let neg_inf = -1./0.;
            values.iter().cloned().fold(neg_inf, f64::max)
        },
        Aggregation::Min => {
            let pos_inf = 1./0.;
            values.iter().cloned().fold(pos_inf, f64::min)
        },
        Aggregation::Sum => {
            values.iter().sum()
        },
        Aggregation::Last => {
            let last = values.last();
            match last {
                Some(&x) => x,
                None => 0.0,
            }
        }
    }
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

#[derive(Debug)]
struct Header {
    meta: Meta,
    archives: Vec<ArchiveInfo>,
}

#[derive(Debug)]
pub enum RRDError {
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

    fn read_header(&self, fd: &mut File) -> Result<Header, RRDError> {
        let meta: Meta = unsafe { from_bytes(fd) };
        let mut archives: Vec<ArchiveInfo> = Vec::with_capacity(meta.num_of_archives as usize);

        for _ in 0..meta.num_of_archives {
            let archive: ArchiveInfo = unsafe { from_bytes(fd) };
            archives.push(archive);
        }

        Ok(Header {
            meta: meta,
            archives: archives,
        })
    }
    fn add_point_to_archive(dp: DataPoint, fd: &mut File, archive: &ArchiveInfo) -> Result<(), RRDError> {
        fd.seek(SeekFrom::Start(archive.offset as u64))?;
        let first: DataPoint = unsafe { from_bytes(fd) };

        // align this datapoint by high archive specs
        let dp = DataPoint {
            time: dp.time - (dp.time % archive.secs_per_point as u64),
            value: dp.value,
        };

        // find the offset to add this datapoint
        let offset = if first.time == 0 {
            archive.offset as u64
        } else {
            let dp_size = mem::size_of::<DataPoint>() as i64;
            let archive_size = archive.num_of_points as i64 * dp_size;
            let skiped_num = (dp.time as i64 - first.time as i64) / archive.secs_per_point as i64;
            let skiped_offset = skiped_num * dp_size as i64;
            let inner_offset = sane_modulo(skiped_offset as i64, archive_size);

            archive.offset as u64 + inner_offset as u64
        };

        let bytes = unsafe { as_bytes(&dp) };
        fd.seek(SeekFrom::Start(offset))?;
        fd.write(bytes)?;
        Ok(())
    }

    fn find_neighbours(dp: DataPoint, high: &ArchiveInfo, low: &&ArchiveInfo, fd: &mut File) -> Result<Vec<DataPoint>, RRDError> {
            fd.seek(SeekFrom::Start(high.offset as u64))?;

            let first: DataPoint = unsafe { from_bytes(fd) };
            let dp_size = mem::size_of::<DataPoint>() as u64;
            let interval = low.secs_per_point as u64;

            let left = dp.time - dp.time % interval;
            let right = left + interval;
            let l_offset = {
                let inner_offset = sane_modulo((left as i64 - first.time as i64)/ high.secs_per_point as i64 * dp_size as i64 ,
                            (high.num_of_points as u64 * dp_size) as i64);
                (high.offset as i64  + inner_offset) as u64
            };

            let r_offset = {
                let inner_offset = sane_modulo((right as i64 - first.time as i64)/ high.secs_per_point as i64 * dp_size as i64 ,
                            (high.num_of_points as u64 * dp_size) as i64);
                (high.offset as i64 + inner_offset) as u64
            };

            let mut high_values: Vec<DataPoint> = vec![];
            if l_offset < r_offset {
                let size = (r_offset - l_offset) as usize / mem::size_of::<u8>();
                let mut buf : Vec<u8> = vec![0u8; size];
                fd.seek(SeekFrom::Start(l_offset))?;
                fd.read_exact(&mut buf)?;
                let datapoints : &[DataPoint] = unsafe {
                    buf.as_slice();
                    slice::from_raw_parts(buf.as_slice().as_ptr() as *const DataPoint, size / mem::size_of::<DataPoint>())
                };
                high_values.extend_from_slice(datapoints);
            } else {
                let archive_size = high.num_of_points as u64 * dp_size;

                let l_size = archive_size as usize + high.offset as usize - l_offset as usize;
                let mut buf: Vec<u8> = vec![0u8; l_size];
                fd.seek(SeekFrom::Start(l_offset))?;
                fd.read_exact(&mut buf)?;
                let datapoints : &[DataPoint] = unsafe {
                    buf.as_slice();
                    slice::from_raw_parts(buf.as_slice().as_ptr() as *const DataPoint, l_size / mem::size_of::<DataPoint>())
                };
                high_values.extend_from_slice(datapoints);

                let r_size = r_offset as usize - high.offset as usize;
                let mut buf: Vec<u8> = vec![0u8; r_size];
                fd.seek(SeekFrom::Start(high.offset as u64))?;
                fd.read_exact(&mut buf)?;
                let datapoints : &[DataPoint] = unsafe {
                    buf.as_slice();
                    slice::from_raw_parts(buf.as_slice().as_ptr() as *const DataPoint, r_size / mem::size_of::<DataPoint>())
                };
                high_values.extend_from_slice(datapoints);
            }

            Ok(high_values)
    }

    pub fn add_point(&mut self, dp: DataPoint) -> Result<(), RRDError> {
        // refuse to add this datapoint if it's in the future
        let now = unix_time();
        if dp.time > now {
            return Err(RRDError::InvalidDataPoint(dp));
        }

        // find all archives which covers this datapoint
        let mut fd = OpenOptions::new().write(true)
            .read(true)
            .create(false)
            .open(self.path.as_ref())?;

        let header = self.read_header(&mut fd)?;
        let delta = now - dp.time;
        let related_archives: Vec<_> = header.archives
            .iter()
            .filter(|e| e.secs_per_point * e.num_of_points > delta as u32)
            .collect();

        // archives are sorted, so the fist has highest precision
        if related_archives.is_empty() {
            return Ok(());
        }
        let high = related_archives[0];

        // align this datapoint by high archive specs
        let dp = DataPoint {
            time: dp.time - (dp.time % high.secs_per_point as u64),
            value: dp.value,
        };

        Self::add_point_to_archive(dp, &mut fd, high)?;

        for low in related_archives.as_slice()[1..].iter() {

            let neighbours =  Self::find_neighbours(dp, high, low, &mut fd)?;
            let neighbours_num = neighbours.len();
            let valid_values: Vec<f64> = neighbours.into_iter().filter(|e| e.time !=0 ).map(|e| e.value).collect();

            let xfill = valid_values.len() as f64 / neighbours_num as f64;
            if xfill >= 0.5 {
                let method = header.meta.aggregation_method;
                let aggreated_value = aggreate(valid_values, method);
                let interval = low.secs_per_point as u64;

                let dp = DataPoint {
                    time: dp.time - dp.time % interval,
                    value: aggreated_value,
                };

                Self::add_point_to_archive(dp, &mut fd, low)?;
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

fn sane_modulo(a: i64, n: i64) -> i64 {
    let m = a % n;

    if m == 0 {
        0
    } else if (a > 0 && n > 0) || (a < 0 && n < 0) {
        m
    } else if (a < 0 && n > 0) || (a > 0 && n < 0) {
        m + n
    } else {
        0
    }
}

#[test]
fn test_read_write_file() {
    let rrd = RRD::new("/tmp/example.rrd");
    let result = rrd.init_file(&[ArchiveSpec {
                                     secs_per_point: 60,
                                     num_of_points: 86400 * 1 / 60,
                                 },
                                 ArchiveSpec {
                                     secs_per_point: 60 * 5,
                                     num_of_points: 288 * 7,
                                 }],
                               Aggregation::Avg);
    assert!(result.is_ok());

    let mut rrd = RRD::new("/tmp/example.rrd");
    let now = unix_time();
    for i in 0..60 * 24 {
        let result = rrd.add_point(DataPoint {
            time: now - i * 60,
            value: 12.3,
        });
        assert!(result.is_ok());
    }
}