use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use byteorder::{LittleEndian, WriteBytesExt};
use rkyv::{Archive, Deserialize, Serialize};
use rocksdb::{MergeOperands, Options, DB};

use sourmash::signature::Signature;
use sourmash::sketch::minhash::{max_hash_for_scaled, KmerMinHash};
use sourmash::sketch::Sketch;

type DatasetID = u64;

fn merge_datasets(
    _: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let original_datasets = existing_val
        .and_then(Datasets::from_slice)
        .unwrap_or_default();
    let mut datasets = original_datasets.clone();

    for op in operands {
        let new_vals = Datasets::from_slice(op).unwrap();
        datasets = Datasets(datasets.0.union(&new_vals.0).cloned().collect());
    }
    //    if let Some(_) = datasets.0.difference(&original_datasets.0).next() {
    datasets.as_bytes()
    //    } else {
    //        None
    //    }
}

fn map_hashes_colors(
    db: Arc<DB>,
    dataset_id: DatasetID,
    search_sig: &Signature,
    threshold: usize,
    template: &Sketch,
    //) -> Option<(HashToColor, Datasets)> {
) {
    let mut search_mh = None;
    if let Some(Sketch::MinHash(mh)) = search_sig.select_sketch(template) {
        search_mh = Some(mh);
    }

    let search_mh = search_mh.expect("Couldn't find a compatible MinHash");
    let colors = Datasets::new(&[dataset_id]).as_bytes().unwrap();

    let matched = search_mh.mins();
    let size = matched.len() as u64;
    if !matched.is_empty() || size > threshold as u64 {
        let mut hash_bytes = [0u8; 8];
        for hash in matched {
            (&mut hash_bytes[..])
                .write_u64::<LittleEndian>(hash)
                .expect("error writing bytes");
            db.merge(&hash_bytes[..], colors.as_slice())
                .expect("error merging");
        }
    }

    /*
        if hash_to_color.is_empty() {
            None
        } else {
            Some((hash_to_color, colors))
        }
    */
}

#[derive(Default, Debug, PartialEq, Clone, Archive, Serialize, Deserialize)]
struct Datasets(HashSet<DatasetID>);

impl Datasets {
    fn new(vals: &[DatasetID]) -> Self {
        Self(HashSet::from_iter(vals.into_iter().cloned()))
    }

    fn from_slice(slice: &[u8]) -> Option<Self> {
        // TODO: avoid the aligned vec allocation here
        let mut vec = rkyv::AlignedVec::new();
        vec.extend_from_slice(slice);
        let archived_value = unsafe { rkyv::archived_root::<Datasets>(vec.as_ref()) };
        let inner = archived_value.deserialize(&mut rkyv::Infallible).unwrap();
        Some(inner)
    }

    fn as_bytes(&self) -> Option<Vec<u8>> {
        let bytes = rkyv::to_bytes::<_, 256>(self).unwrap();
        Some(bytes.into_vec())

        /*
        let mut serializer = DefaultSerializer::default();
        let v = serializer.serialize_value(self).unwrap();
        debug_assert_eq!(v, 0);
        let buf = serializer.into_serializer().into_inner();
        debug_assert!(Datasets::from_slice(&buf.to_vec()).is_some());
        Some(buf.to_vec())
        */
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let n = Path::new("_rocksdb_sourmash_test");

    let max_hash = max_hash_for_scaled(10000);
    let template = Sketch::MinHash(
        KmerMinHash::builder()
            .num(0u32)
            .ksize(31)
            .max_hash(max_hash)
            .build(),
    );
    let search_sigs: [PathBuf; 3] = [
        "tests/test-data/GCF_000006945.2_ASM694v2_genomic.fna.gz.sig".into(),
        "tests/test-data/GCF_000007545.1_ASM754v1_genomic.fna.gz.sig".into(),
        "tests/test-data/GCF_000008105.1_ASM810v1_genomic.fna.gz.sig".into(),
    ];

    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_merge_operator_associative("datasets operator", merge_datasets);
    //opts.set_compaction_style(DBCompactionStyle::Universal);
    //opts.set_min_write_buffer_number_to_merge(10);
    {
        let db = Arc::new(DB::open(&opts, &n).unwrap());

        let threshold = 0;

        let processed_sigs = AtomicUsize::new(0);
        //let sig_iter = search_sigs.par_iter();
        let sig_iter = search_sigs.iter();

        let _filtered_sigs = sig_iter
            .enumerate()
            .filter_map(|(dataset_id, filename)| {
                let i = processed_sigs.fetch_add(1, Ordering::SeqCst);
                if i % 1000 == 0 {
                    eprintln!("Processed {} reference sigs", i);
                }

                let search_sig = Signature::from_path(&filename)
                    .unwrap_or_else(|_| panic!("Error processing {:?}", filename))
                    .swap_remove(0);

                map_hashes_colors(
                    db.clone(),
                    dataset_id as DatasetID,
                    &search_sig,
                    threshold,
                    &template,
                );
                Some(true)
            })
            .count();

        eprintln!("Processed {} reference sigs", processed_sigs.into_inner());

        /*
        let mut hash_bytes = [0u8; 8];
        (&mut hash_bytes[..])
            .write_u64::<LittleEndian>(1078036129600)
            .expect("error writing bytes");
        let r = db.get(&hash_bytes[..])?;
        assert_eq!(
            Datasets::from_slice(&r.unwrap()).unwrap(),
            Datasets::new(&[1, 2, 3, 4, 5, 6])
        );
        */

        /*
        use byteorder::ReadBytesExt;
        let mut iter = db.iterator(rocksdb::IteratorMode::Start); // Always iterates forward
        for (key, value) in iter {
            let k = (&key[..]).read_u64::<LittleEndian>().unwrap();
            println!("Saw {} {:?}", k, Datasets::from_slice(&value));
            //println!("Saw {} {:?}", k, value);
        }
        */
    }
    //let _ = DB::destroy(&opts, n);
    Ok(())
}
