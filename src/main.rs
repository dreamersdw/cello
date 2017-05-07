mod rrd;

use rrd::{RRD, ArchiveSpec, Aggregation};

fn main() {
    let rrd = RRD::new("/tmp/example.rrd");
    let result = rrd.init_file(&[ArchiveSpec {
                                     secs_per_point: 60,
                                     num_of_points: 86400 / 60,
                                 }],
                               Aggregation::Avg);
    result.unwrap();
}
