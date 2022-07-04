use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::Path;
use std::sync::Arc;

use rkyv::{archived_root, ser::Serializer, Archive, Deserialize, Serialize};
use rocksdb::{ColumnFamilyDescriptor, MergeOperands, Options, DB, DEFAULT_COLUMN_FAMILY_NAME};

const SCRATCH_SIZE: usize = 256;
type DefaultSerializer = rkyv::ser::serializers::AllocSerializer<SCRATCH_SIZE>;
type DefaultDeserializer = rkyv::Infallible;
type DatasetID = u64;

fn merge_datasets(
    _: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut datasets = existing_val
        .and_then(Datasets::from_slice)
        .unwrap_or_default();

    for op in operands {
        let new_vals = Datasets::from_slice(op).unwrap();
        datasets = Datasets(datasets.0.union(&new_vals.0).cloned().collect());
    }
    datasets.as_bytes()
}

#[derive(Default, Debug, PartialEq, Archive, Serialize, Deserialize)]
struct Datasets(HashSet<DatasetID>);

impl Datasets {
    fn new(vals: &[DatasetID]) -> Self {
        Self(HashSet::from_iter(vals.into_iter().cloned()))
    }

    fn from_slice(slice: &[u8]) -> Option<Self> {
        let mut deserializer = DefaultDeserializer::default();
        let archived_value = unsafe { archived_root::<Datasets>(slice) };
        let inner = archived_value.deserialize(&mut deserializer).unwrap();
        Some(inner)
    }

    fn as_bytes(&self) -> Option<Vec<u8>> {
        let mut serializer = DefaultSerializer::default();
        serializer.serialize_value(self).unwrap();
        let buf = serializer.into_serializer().into_inner();
        Some(buf.to_vec())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let n = Path::new("_rocksdb_sourmash_test");

    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_merge_operator_associative("datasets operator", merge_datasets);
    //opts.set_compaction_style(DBCompactionStyle::Universal);
    //opts.set_min_write_buffer_number_to_merge(10);
    {
        let db = Arc::new(DB::open(&opts, &n).unwrap());

        let p = db.put(b"k1", Datasets::new(&[1]).as_bytes().unwrap())?;
        db.merge(b"k1", Datasets::new(&[2]).as_bytes().unwrap())?;
        db.merge(b"k1", Datasets::new(&[3]).as_bytes().unwrap())?;
        db.merge(b"k1", Datasets::new(&[4, 5, 6]).as_bytes().unwrap())?;

        let r = db.get(b"k1")?;
        assert_eq!(
            Datasets::from_slice(&r.unwrap()).unwrap(),
            Datasets::new(&[1, 2, 3, 4, 5, 6])
        );
    }
    let _ = DB::destroy(&opts, n);
    Ok(())
}
