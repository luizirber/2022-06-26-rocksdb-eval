use std::path::PathBuf;
use std::sync::Arc;

use byteorder::{LittleEndian, WriteBytesExt};
use log::info;
use rocksdb::{ColumnFamilyDescriptor, MergeOperands, Options};

use sourmash::signature::Signature;
use sourmash::sketch::minhash::KmerMinHash;
use sourmash::sketch::Sketch;

use crate::prepare_query;
use crate::{sig_save_to_db, DatasetID, Datasets, SigCounter, DB, HASHES, SIGS};

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
        datasets.union(new_vals);
    }
    // TODO: optimization! if nothing changed, skip as_bytes()
    datasets.as_bytes()
}

pub fn map_hashes_colors(
    db: Arc<DB>,
    dataset_id: DatasetID,
    filename: &PathBuf,
    threshold: f64,
    template: &Sketch,
    save_paths: bool,
) {
    let search_sig = Signature::from_path(&filename)
        .unwrap_or_else(|_| panic!("Error processing {:?}", filename))
        .swap_remove(0);

    let search_mh =
        prepare_query(&search_sig, template).expect("Couldn't find a compatible MinHash");

    let colors = Datasets::new(&[dataset_id]).as_bytes().unwrap();

    let cf_hashes = db.cf_handle(HASHES).unwrap();

    let matched = search_mh.mins();
    let size = matched.len() as u64;
    if !matched.is_empty() || size > threshold as u64 {
        // FIXME threshold is f64
        let mut hash_bytes = [0u8; 8];
        for hash in matched {
            (&mut hash_bytes[..])
                .write_u64::<LittleEndian>(hash)
                .expect("error writing bytes");
            db.merge_cf(&cf_hashes, &hash_bytes[..], colors.as_slice())
                .expect("error merging");
        }
    }

    sig_save_to_db(
        db.clone(),
        search_sig,
        search_mh,
        size,
        threshold,
        save_paths,
        filename,
        dataset_id,
    );
}

pub fn counter_for_query(db: Arc<DB>, query: &KmerMinHash) -> SigCounter {
    info!("Collecting hashes");
    let cf_hashes = db.cf_handle(HASHES).unwrap();
    let hashes_iter = query.iter_mins().map(|hash| {
        let mut v = vec![0_u8; 8];
        (&mut v[..])
            .write_u64::<LittleEndian>(*hash)
            .expect("error writing bytes");
        (&cf_hashes, v)
    });

    info!("Multi get");
    db.multi_get_cf(hashes_iter)
        .into_iter()
        .filter_map(|r| r.ok().unwrap_or(None))
        .flat_map(|raw_datasets| {
            let new_vals = Datasets::from_slice(&raw_datasets).unwrap();
            new_vals.into_iter()
        })
        .collect()
}

pub fn cf_descriptors() -> Vec<ColumnFamilyDescriptor> {
    let mut cfopts = Options::default();
    cfopts.set_max_write_buffer_number(16);
    cfopts.set_merge_operator_associative("datasets operator", merge_datasets);
    cfopts.set_min_write_buffer_number_to_merge(10);

    // Updated default from
    // https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning#other-general-options
    cfopts.set_level_compaction_dynamic_level_bytes(true);

    let cf_hashes = ColumnFamilyDescriptor::new(HASHES, cfopts);

    let mut cfopts = Options::default();
    cfopts.set_max_write_buffer_number(16);
    // Updated default
    cfopts.set_level_compaction_dynamic_level_bytes(true);
    //cfopts.set_merge_operator_associative("colors operator", merge_colors);

    let cf_sigs = ColumnFamilyDescriptor::new(SIGS, cfopts);

    vec![cf_hashes, cf_sigs]
}
