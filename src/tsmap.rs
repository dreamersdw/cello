use rrd::DataPoint;
use std::borrow::Cow;
use std::clone::Clone;
use std::collections::HashMap;
use std::iter::Iterator;

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
