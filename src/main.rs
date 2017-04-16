mod rrd;
mod tsmap;

use rrd::{RRD, ArchiveSpec, Aggregation};

fn main() {
    let rrd = RRD::new("/tmp/example.rrd");
    let result = rrd.init_file(&[ArchiveSpec {
                                     secs_per_point: 60,
                                     num_of_points: 86400 * 1 / 60,
                                 }],
                               Aggregation::Avg);
}
