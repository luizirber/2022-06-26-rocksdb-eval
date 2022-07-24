use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use byteorder::{LittleEndian, WriteBytesExt};
use log::{info, trace};
use rayon::prelude::*;
use rocksdb::{ColumnFamilyDescriptor, MergeOperands, Options};

use sourmash::index::revindex::GatherResult;
use sourmash::signature::{Signature, SigsTrait};
use sourmash::sketch::minhash::KmerMinHash;
use sourmash::sketch::Sketch;

use crate::color_revindex::Colors;
use crate::prepare_query;
use crate::{
    sig_save_to_db, stats_for_cf, DatasetID, Datasets, HashToColor, QueryColors, SigCounter,
    SignatureData, DB, HASHES, SIGS,
};

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

pub fn prepare_gather_counters(
    db: Arc<DB>,
    query: &KmerMinHash,
) -> (SigCounter, QueryColors, HashToColor) {
    let cf_hashes = db.cf_handle(HASHES).unwrap();
    let hashes_iter = query.iter_mins().map(|hash| {
        let mut v = vec![0_u8; 8];
        (&mut v[..])
            .write_u64::<LittleEndian>(*hash)
            .expect("error writing bytes");
        (&cf_hashes, v)
    });

    /*
     build a HashToColors for query,
     and a QueryColors (Color -> Datasets) mapping.
     Loading Datasets from rocksdb for every hash takes too long.
    */
    let mut query_colors: QueryColors = Default::default();
    let mut counter: SigCounter = Default::default();

    info!("Building hash_to_colors and query_colors");
    let hash_to_colors = query
        .iter_mins()
        .zip(db.multi_get_cf(hashes_iter).into_iter())
        .filter_map(|(k, r)| {
            let raw = r.ok().unwrap_or(None);
            if !raw.is_none() {
                let new_vals = Datasets::from_slice(&raw.unwrap()).unwrap();
                let color = Colors::compute_color(&new_vals);
                query_colors.entry(color).or_insert(new_vals.clone());
                counter.update(new_vals.into_iter());
                Some((*k, color))
            } else {
                None
            }
        })
        .collect();

    (counter, query_colors, hash_to_colors)
}

pub fn matches_from_counter(db: Arc<DB>, counter: SigCounter, threshold: usize) -> Vec<String> {
    let cf_sigs = db.cf_handle(SIGS).unwrap();

    let matches_iter = counter
        .most_common()
        .into_iter()
        .filter_map(|(dataset_id, size)| {
            if size >= threshold {
                let mut v = vec![0_u8; 8];
                (&mut v[..])
                    .write_u64::<LittleEndian>(dataset_id)
                    .expect("error writing bytes");
                Some((&cf_sigs, v))
            } else {
                None
            }
        });

    info!("Multi get matches");
    db.multi_get_cf(matches_iter)
        .into_par_iter()
        .filter_map(|r| r.ok().unwrap_or(None))
        .filter_map(
            |sigdata| match SignatureData::from_slice(&sigdata).unwrap() {
                SignatureData::Empty => None,
                SignatureData::External(p) => Some(p),
                SignatureData::Internal(sig) => Some(sig.name()),
            },
        )
        .collect()
}

pub fn gather(
    db: Arc<DB>,
    mut counter: SigCounter,
    query_colors: QueryColors,
    hash_to_color: HashToColor,
    threshold: usize,
    orig_query: &KmerMinHash,
    template: &Sketch,
) -> Result<Vec<GatherResult>, Box<dyn std::error::Error>> {
    let mut match_size = usize::max_value();
    let mut matches = vec![];
    let mut key_bytes = [0u8; 8];
    //let mut query: KmerMinHashBTree = orig_query.clone().into();

    let cf_sigs = db.cf_handle(SIGS).unwrap();

    while match_size > threshold && !counter.is_empty() {
        trace!("counter len: {}", counter.len());
        trace!("match size: {}", match_size);

        let (dataset_id, size) = counter.k_most_common_ordered(1)[0];
        match_size = if size >= threshold { size } else { break };

        (&mut key_bytes[..])
            .write_u64::<LittleEndian>(dataset_id)
            .expect("error writing bytes");

        let match_sig = db
            .get_cf(&cf_sigs, &key_bytes[..])
            .ok()
            .map(
                |sigdata| match SignatureData::from_slice(&(sigdata.unwrap())).unwrap() {
                    SignatureData::Empty => todo!("throw error, empty sig"),
                    SignatureData::External(_p) => todo!("Load from external"),
                    SignatureData::Internal(sig) => sig,
                },
            )
            .expect(format!("Unknown dataset {}", dataset_id).as_ref());

        let match_mh =
            prepare_query(&match_sig, template).expect("Couldn't find a compatible MinHash");

        // Calculate stats
        let f_orig_query = match_size as f64 / orig_query.size() as f64;
        let f_match = match_size as f64 / match_mh.size() as f64;
        let name = match_sig.name();
        let unique_intersect_bp = match_mh.scaled() as usize * match_size;
        let gather_result_rank = matches.len();

        let (intersect_orig, _) = match_mh.intersection_size(&orig_query)?;
        let intersect_bp = (match_mh.scaled() as u64 * intersect_orig) as usize;

        let f_unique_to_query = intersect_orig as f64 / orig_query.size() as f64;
        let match_ = match_sig.clone();
        let md5 = match_sig.md5sum();

        // TODO: all of these
        let filename = "".into();
        let f_unique_weighted = 0.;
        let average_abund = 0;
        let median_abund = 0;
        let std_abund = 0;
        let f_match_orig = 0.;
        let remaining_bp = 0;

        let result = GatherResult::builder()
            .intersect_bp(intersect_bp)
            .f_orig_query(f_orig_query)
            .f_match(f_match)
            .f_unique_to_query(f_unique_to_query)
            .f_unique_weighted(f_unique_weighted)
            .average_abund(average_abund)
            .median_abund(median_abund)
            .std_abund(std_abund)
            .filename(filename)
            .name(name)
            .md5(md5)
            .match_(match_)
            .f_match_orig(f_match_orig)
            .unique_intersect_bp(unique_intersect_bp)
            .gather_result_rank(gather_result_rank)
            .remaining_bp(remaining_bp)
            .build();
        matches.push(result);

        trace!("Preparing counter for next round");
        // Prepare counter for finding the next match by decrementing
        // all hashes found in the current match in other datasets
        // TODO: not used at the moment, so just skip.
        //query.remove_many(match_mh.to_vec().as_slice())?;

        // TODO: Use HashesToColors here instead. If not initialized,
        //       build it.
        match_mh
            .iter_mins()
            .filter_map(|hash| hash_to_color.get(hash))
            .flat_map(|color| {
                // TODO: remove this clone
                query_colors.get(color).unwrap().clone().into_iter()
            })
            .for_each(|dataset| {
                // TODO: collect the flat_map into a Counter, and remove more
                //       than one at a time...
                counter.entry(dataset).and_modify(|e| {
                    if *e > 0 {
                        *e -= 1
                    }
                });
            });

        counter.remove(&dataset_id);
    }
    Ok(matches)
}

pub fn index(
    db: Arc<DB>,
    index_sigs: Vec<PathBuf>,
    template: &Sketch,
    threshold: f64,
    save_paths: bool,
) {
    let processed_sigs = AtomicUsize::new(0);

    index_sigs
        .par_iter()
        .enumerate()
        .for_each(|(dataset_id, filename)| {
            let i = processed_sigs.fetch_add(1, Ordering::SeqCst);
            if i % 1000 == 0 {
                info!("Processed {} reference sigs", i);
            }

            map_hashes_colors(
                db.clone(),
                dataset_id as DatasetID,
                filename,
                threshold,
                &template,
                save_paths,
            );
        });
    info!("Processed {} reference sigs", processed_sigs.into_inner());
}

pub fn check(db: Arc<DB>, quick: bool) {
    stats_for_cf(db.clone(), HASHES, true, quick);
    info!("");
    stats_for_cf(db.clone(), SIGS, false, quick);
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
