use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use byteorder::{LittleEndian, WriteBytesExt};
use log::{debug, error, info, trace};
use rayon::prelude::*;
use rkyv::{Archive, Deserialize, Serialize};
use rocksdb::{ColumnFamilyDescriptor, MergeOperands, Options};

use sourmash::signature::Signature;
use sourmash::sketch::minhash::KmerMinHash;
use sourmash::sketch::Sketch;

use crate::prepare_query;
use crate::{
    sig_save_to_db, stats_for_cf, DatasetID, Datasets, RevIndex, SigCounter, SignatureData, COLORS,
    DB, HASHES, SIGS,
};

/*
enum ColorCollision {
    Unique(Datasets),
    Many(Vec<Datasets>),
}

fn merge_colors(
    existing_color: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut colisions = existing_val
        .and_then(ColorColision::from_slice)
        .unwrap_or_default();

    for op in operands {
        let new_vals = Datasets::from_slice(op).unwrap();
        datasets.union(new_vals);
    }
    // TODO: optimization! if nothing changed, skip as_bytes()
    datasets.as_bytes()
}
*/

#[derive(Debug, Clone)]
pub struct ColorRevIndex {
    pub(crate) db: Arc<DB>,
    rx_merge: flume::Receiver<(Option<Color>, Datasets)>,
    tx_colors: flume::Sender<Color>,
}

/* TODO: need the repair_cf variant, not available in rocksdb-rust yet
pub fn repair(path: &Path) {
    let opts = db_options();

    DB::repair(&opts, path).unwrap()
}
*/

impl ColorRevIndex {
    fn merge_datasets(
        _: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        trace!("triggering merge datasets");
        let mut datasets = existing_val
            .and_then(Datasets::from_slice)
            .unwrap_or_default();

        for op in operands {
            let new_vals = Datasets::from_slice(op).unwrap();
            datasets.union(new_vals);
        }

        datasets.as_bytes()
    }

    pub fn open(path: &Path, read_only: bool) -> RevIndex {
        let mut opts = crate::RevIndex::db_options();

        if !read_only {
            opts.create_if_missing(true);
            //opts.create_missing_column_families(true);
        }

        let (tx_merge, rx_merge) = flume::unbounded();
        let (tx_colors, rx_colors) = flume::unbounded();

        let merge_colors = move |_: &[u8],
                                 existing_val: Option<&[u8]>,
                                 operands: &MergeOperands| {
            trace!("triggering merge colors");
            use byteorder::ReadBytesExt;

            let current_color = existing_val.map(|c| (&c[..]).read_u64::<LittleEndian>().unwrap());

            let mut datasets: Datasets = Default::default();

            for op in operands {
                let new_vals = Datasets::from_slice(op).unwrap();
                datasets.union(new_vals);
            }

            trace!("sending current_color {:?}", current_color);
            tx_merge
                .send((current_color, datasets))
                .expect("Error sending current_color");

            trace!("receiving new color for current_color {:?}", current_color);
            let new_color = rx_colors.recv().expect("Error receiving new color");
            trace!("received new_color {}", new_color);
            let mut color_bytes = vec![0u8; 8];
            (&mut color_bytes[..])
                .write_u64::<LittleEndian>(new_color)
                .expect("error writing bytes");

            Some(color_bytes)
        };

        let mut cfopts = Options::default();
        cfopts.set_max_write_buffer_number(16);
        cfopts.set_merge_operator(
            "datasets operator",
            merge_colors,         // full
            Self::merge_datasets, // partial
        );
        cfopts.set_min_write_buffer_number_to_merge(10);

        // Updated default from
        // https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning#other-general-options
        cfopts.set_level_compaction_dynamic_level_bytes(true);

        let cf_hashes = ColumnFamilyDescriptor::new(HASHES, cfopts);

        let mut cfopts = Options::default();
        cfopts.set_max_write_buffer_number(16);
        // Updated default
        cfopts.set_level_compaction_dynamic_level_bytes(true);

        let cf_colors = ColumnFamilyDescriptor::new(COLORS, cfopts);

        let mut cfopts = Options::default();
        cfopts.set_max_write_buffer_number(16);
        // Updated default
        cfopts.set_level_compaction_dynamic_level_bytes(true);
        //cfopts.set_merge_operator_associative("colors operator", merge_colors);

        let cf_sigs = ColumnFamilyDescriptor::new(SIGS, cfopts);

        let cfs = vec![cf_hashes, cf_sigs, cf_colors];

        let db = if read_only {
            //TODO: error_if_log_file_exists set to false, is that an issue?
            Arc::new(DB::open_cf_descriptors_read_only(&opts, path, cfs, false).unwrap())
        } else {
            Arc::new(DB::open_cf_descriptors(&opts, path, cfs).unwrap())
        };

        RevIndex::Color(Self {
            db,
            rx_merge,
            tx_colors,
        })
    }

    fn map_hashes_colors(
        &self,
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

        let cf_hashes = self.db.cf_handle(HASHES).unwrap();

        let matched = search_mh.mins();
        let size = matched.len() as u64;
        if !matched.is_empty() || size > threshold as u64 {
            // FIXME threshold is f64
            let mut hash_bytes = [0u8; 8];
            for hash in matched {
                (&mut hash_bytes[..])
                    .write_u64::<LittleEndian>(hash)
                    .expect("error writing bytes");

                self.db
                    .merge_cf(&cf_hashes, &hash_bytes[..], colors.as_slice())
                    .expect("error merging");
            }
        }

        sig_save_to_db(
            self.db.clone(),
            search_sig,
            search_mh,
            size,
            threshold,
            save_paths,
            filename,
            dataset_id,
        );
    }

    pub fn counter_for_query(&self, query: &KmerMinHash) -> SigCounter {
        let finished = Arc::new(AtomicBool::new(false));
        let color_writer = self.set_up_color_writer(finished.clone());

        info!("Collecting hashes");
        let cf_hashes = self.db.cf_handle(HASHES).unwrap();
        let hashes_iter = query.iter_mins().map(|hash| {
            let mut v = vec![0_u8; 8];
            (&mut v[..])
                .write_u64::<LittleEndian>(*hash)
                .expect("error writing bytes");
            (&cf_hashes, v)
        });

        info!("Multi get hashes");
        let cf_colors = self.db.cf_handle(COLORS).unwrap();
        let colors_iter = self
            .db
            .multi_get_cf(hashes_iter)
            .into_iter()
            .filter_map(|r| r.ok().unwrap_or(None).map(|color| (&cf_colors, color)));

        info!("Multi get colors");
        let counter = self
            .db
            .multi_get_cf(colors_iter)
            .into_iter()
            .filter_map(|r| r.ok().unwrap_or(None))
            .flat_map(|datasets| {
                let new_vals = Datasets::from_slice(&datasets).unwrap();
                new_vals.into_iter()
            })
            .collect();
        finished.store(true, Ordering::Relaxed);

        if let Err(e) = color_writer.join() {
            error!("Unable to join internal thread: {:?}", e);
        };

        counter
    }

    pub fn matches_from_counter(self, counter: SigCounter, threshold: usize) -> Vec<String> {
        let finished = Arc::new(AtomicBool::new(false));
        let color_writer = self.set_up_color_writer(finished.clone());

        let cf_sigs = self.db.cf_handle(SIGS).unwrap();

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
        let matches = self
            .db
            .multi_get_cf(matches_iter)
            .into_iter()
            .filter_map(|r| r.ok().unwrap_or(None))
            .filter_map(
                |sigdata| match SignatureData::from_slice(&sigdata).unwrap() {
                    SignatureData::Empty => None,
                    SignatureData::External(p) => Some(p),
                    SignatureData::Internal(sig) => Some(sig.name()),
                },
            )
            .collect();
        finished.store(true, Ordering::Relaxed);

        if let Err(e) = color_writer.join() {
            error!("Unable to join internal thread: {:?}", e);
        };

        matches
    }

    fn set_up_color_writer(&self, finished_color: Arc<AtomicBool>) -> std::thread::JoinHandle<()> {
        let color_db = self.db.clone();
        let tx_colors = self.tx_colors.clone();
        let rx_merge = self.rx_merge.clone();

        std::thread::spawn(move || {
            debug!("color thread spawned");
            let mut color_bytes = [0u8; 8];

            debug!("waiting for colors");
            while !finished_color.load(Ordering::Relaxed) {
                for (current_color, new_idx) in rx_merge.try_iter() {
                    trace!("received color {:?}", current_color);
                    let new_idx: Vec<_> = new_idx.into_iter().collect();
                    let new_color =
                        Colors::update(color_db.clone(), current_color, new_idx.as_slice())
                            .unwrap();

                    (&mut color_bytes[..])
                        .write_u64::<LittleEndian>(new_color)
                        .expect("error writing bytes");

                    tx_colors.send(new_color).expect("error sending color");
                    trace!("sent color {}", new_color);
                }
            }
            debug!("Finishing colors thread");
        })
    }

    pub fn index(
        &self,
        index_sigs: Vec<PathBuf>,
        template: &Sketch,
        threshold: f64,
        save_paths: bool,
    ) {
        let finished = Arc::new(AtomicBool::new(false));
        let color_writer = self.set_up_color_writer(finished.clone());

        let processed_sigs = AtomicUsize::new(0);
        index_sigs
            .par_iter()
            .enumerate()
            .for_each(|(dataset_id, filename)| {
                let i = processed_sigs.fetch_add(1, Ordering::SeqCst);
                if i % 1000 == 0 {
                    info!("Processed {} reference sigs", i);
                }

                self.map_hashes_colors(
                    dataset_id as DatasetID,
                    filename,
                    threshold,
                    template,
                    save_paths,
                );
            });
        info!("Processed {} reference sigs", processed_sigs.into_inner());

        info!("Compressing colors");
        Colors::compress(self.db.clone());
        info!("Finished compressing colors");

        self.compact();
        self.flush().unwrap();

        finished.store(true, Ordering::Relaxed);

        if let Err(e) = color_writer.join() {
            error!("Unable to join internal thread: {:?}", e);
        };
    }

    pub fn compact(&self) {
        for cf_name in [HASHES, SIGS, COLORS] {
            let cf = self.db.cf_handle(cf_name).unwrap();
            self.db.compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>)
        }
    }

    pub fn flush(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.db.flush_wal(true)?;

        for cf_name in [HASHES, SIGS, COLORS] {
            let cf = self.db.cf_handle(cf_name).unwrap();
            self.db.flush_cf(&cf)?;
        }

        Ok(())
    }

    pub fn check(&self, quick: bool) {
        let finished = Arc::new(AtomicBool::new(false));
        let color_writer = self.set_up_color_writer(finished.clone());

        stats_for_cf(self.db.clone(), HASHES, false, quick);
        info!("");
        stats_for_cf(self.db.clone(), COLORS, true, quick);
        info!("");
        stats_for_cf(self.db.clone(), SIGS, false, quick);
        finished.store(true, Ordering::Relaxed);

        if let Err(e) = color_writer.join() {
            error!("Unable to join internal thread: {:?}", e);
        };
    }
}

use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};
pub type Color = u64;

#[derive(Default, Debug, PartialEq, Clone, Archive, Serialize, Deserialize)]
pub struct Colors;

impl Colors {
    /// Given a color and a new idx, return an updated color
    ///
    /// This might create a new one, or find an already existing color
    /// that contains the new_idx
    ///
    /// Future optimization: store a count for each color, so we can track
    /// if there are extra colors that can be removed at the end.
    /// (the count is decreased whenever a new color has to be created)
    pub fn update<'a, I: IntoIterator<Item = &'a DatasetID>>(
        db: Arc<DB>,
        current_color: Option<Color>,
        new_idxs: I,
    ) -> Result<Color, Box<dyn std::error::Error>> {
        let cf_colors = db.cf_handle(COLORS).unwrap();

        if let Some(color) = current_color {
            let mut color_bytes = [0u8; 8];
            (&mut color_bytes[..])
                .write_u64::<LittleEndian>(color)
                .expect("error writing bytes");

            if let Some(idxs) = db.get_cf(&cf_colors, &color_bytes)? {
                let idxs = Datasets::from_slice(&idxs).unwrap();
                let idx_to_add: Vec<_> = new_idxs
                    .into_iter()
                    .filter(|new_idx| !idxs.contains(new_idx))
                    .collect();

                if idx_to_add.is_empty() {
                    // Easy case, it already has all the new_idxs, so just return this color
                    Ok(color)
                } else {
                    // We need to either create a new color,
                    // or find an existing color that have the same idxs

                    let mut idxs = idxs.clone();
                    idxs.extend(idx_to_add.into_iter().cloned());
                    let new_color = Colors::compute_color(&idxs);

                    (&mut color_bytes[..])
                        .write_u64::<LittleEndian>(new_color)
                        .expect("error writing bytes");

                    if matches!(db.get_cf(&cf_colors, &color_bytes)?, None) {
                        // The color doesn't exist yet, so let's add it

                        // TODO: deal with collisions?
                        db.put_cf(
                            &cf_colors,
                            &color_bytes[..],
                            idxs.as_bytes().expect("Error converting color"),
                        )
                        .expect("error merging color");
                    }

                    Ok(new_color)
                }
            } else {
                use byteorder::ReadBytesExt;

                let iter = db.iterator_cf(&cf_colors, rocksdb::IteratorMode::Start);
                for (key, value) in iter {
                    let k = (&key[..]).read_u64::<LittleEndian>().unwrap();
                    let v = Datasets::from_slice(&value).expect("Error with value");
                    dbg!((k, v));
                }
                unimplemented!("throw error, current_color must exist in order to be updated. current_color: {:?}", current_color);
            }
        } else {
            let mut idxs = Datasets::default();
            idxs.extend(new_idxs.into_iter().cloned());
            let new_color = Colors::compute_color(&idxs);

            let mut color_bytes = [0u8; 8];
            (&mut color_bytes[..])
                .write_u64::<LittleEndian>(new_color)
                .expect("error writing bytes");

            // TODO: deal with collisions?
            db.put_cf(
                &cf_colors,
                &color_bytes[..],
                idxs.as_bytes().expect("Error converting color"),
            )
            .expect("error merging color");
            Ok(new_color)
        }
    }

    pub fn compute_color(idxs: &Datasets) -> Color {
        let s = BuildHasherDefault::<twox_hash::Xxh3Hash128>::default();
        let mut hasher = s.build_hasher();
        /*
        // TODO: remove this...
        let mut sorted: Vec<_> = idxs.iter().collect();
        sorted.sort();
        */
        idxs.hash(&mut hasher);
        hasher.finish()
    }

    pub fn compress(db: Arc<DB>) {
        use byteorder::ReadBytesExt;

        let cf_colors = db.cf_handle(COLORS).unwrap();
        let cf_hashes = db.cf_handle(HASHES).unwrap();

        let mut colors: std::collections::HashSet<Color> = Default::default();

        debug!("Collecting colors");
        let iter = db.iterator_cf(&cf_hashes, rocksdb::IteratorMode::Start);
        for (_, value) in iter {
            let color = (&value[..]).read_u64::<LittleEndian>().unwrap();
            colors.insert(color);
        }

        debug!("Deleting unused colors");
        let mut total_deletions: usize = 0;
        let iter = db.iterator_cf(&cf_colors, rocksdb::IteratorMode::Start);
        for (key, _) in iter {
            let k = (&key[..]).read_u64::<LittleEndian>().unwrap();
            if !colors.contains(&k) {
                db.delete_cf(&cf_colors, &key[..]).unwrap();
                total_deletions += 1;
                if total_deletions % 1000 == 0 {
                    debug!("Deleted {} colors", total_deletions);
                }
            }
        }
        debug!("Finished compression");
    }
}
