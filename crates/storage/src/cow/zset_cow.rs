use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use bytes::Bytes;
use ordered_float::OrderedFloat;
use parking_lot::RwLock;
use std::collections::HashMap;

pub struct ZSet {
    pub scores: HashMap<Bytes, f64>,
    pub by_score: BTreeMap<OrderedFloat<f64>, HashSet<Bytes>>,
}

pub struct ZSetLayer {
    pub updated: HashMap<Bytes, f64>,
    pub removed: HashMap<Bytes, f64>,
}

pub struct ZSetCow {
    pub layers: [Arc<RwLock<ZSetLayer>>; 10],
}
