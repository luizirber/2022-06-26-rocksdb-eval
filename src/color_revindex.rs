use std::path::PathBuf;
use std::sync::Arc;

use byteorder::{LittleEndian, WriteBytesExt};
use log::info;
use rkyv::{Archive, Deserialize, Serialize};
use rocksdb::{ColumnFamilyDescriptor, Options};

use sourmash::signature::Signature;
use sourmash::sketch::minhash::KmerMinHash;
use sourmash::sketch::Sketch;

use crate::prepare_query;
use crate::{sig_save_to_db, DatasetID, Datasets, SigCounter, COLORS, DB, HASHES, SIGS};

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

pub fn map_hashes_colors(
    db: Arc<DB>,
    dataset_id: DatasetID,
    filename: &PathBuf,
    threshold: f64,
    template: &Sketch,
    save_paths: bool,
) {
    use byteorder::ReadBytesExt;

    let search_sig = Signature::from_path(&filename)
        .unwrap_or_else(|_| panic!("Error processing {:?}", filename))
        .swap_remove(0);

    let search_mh =
        prepare_query(&search_sig, template).expect("Couldn't find a compatible MinHash");

    let cf_hashes = db.cf_handle(HASHES).unwrap();

    let matched = search_mh.mins();
    let size = matched.len() as u64;
    if !matched.is_empty() || size > threshold as u64 {
        // FIXME threshold is f64
        let mut hash_bytes = [0u8; 8];
        let mut color_bytes = [0u8; 8];
        for hash in matched {
            (&mut hash_bytes[..])
                .write_u64::<LittleEndian>(hash)
                .expect("error writing bytes");

            let current_color = if let Ok(Some(c)) = db.get_cf(&cf_hashes, &hash_bytes[..]) {
                Some((&c[..]).read_u64::<LittleEndian>().unwrap())
            } else {
                None
            };

            let new_color =
                Colors::update(db.clone(), current_color, &[dataset_id as DatasetID]).unwrap();

            (&mut color_bytes[..])
                .write_u64::<LittleEndian>(new_color)
                .expect("error writing bytes");

            db.put_cf(&cf_hashes, &hash_bytes[..], &color_bytes[..])
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

    info!("Multi get hashes");
    let cf_colors = db.cf_handle(COLORS).unwrap();
    let colors_iter = db
        .multi_get_cf(hashes_iter)
        .into_iter()
        .filter_map(|r| r.ok().unwrap_or(None).map(|color| (&cf_colors, color)));

    info!("Multi get colors");
    db.multi_get_cf(colors_iter)
        .into_iter()
        .filter_map(|r| r.ok().unwrap_or(None))
        .flat_map(|datasets| {
            let new_vals = Datasets::from_slice(&datasets).unwrap();
            new_vals.into_iter()
        })
        .collect()
}

use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};
type Color = u64;

#[derive(Default, Debug, PartialEq, Clone, Archive, Serialize, Deserialize)]
pub struct Colors;

impl Colors {
    pub fn new() -> Colors {
        Default::default()
    }

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

    fn compute_color(idxs: &Datasets) -> Color {
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

        let iter = db.iterator_cf(&cf_hashes, rocksdb::IteratorMode::Start);
        for (_, value) in iter {
            let color = (&value[..]).read_u64::<LittleEndian>().unwrap();
            colors.insert(color);
        }

        let iter = db.iterator_cf(&cf_colors, rocksdb::IteratorMode::Start);
        for (key, _) in iter {
            let k = (&key[..]).read_u64::<LittleEndian>().unwrap();
            if !colors.contains(&k) {
                db.delete_cf(&cf_colors, &key[..]).unwrap();
            }
        }
    }
}

pub fn cf_descriptors() -> Vec<ColumnFamilyDescriptor> {
    let mut cfopts = Options::default();
    cfopts.set_max_write_buffer_number(16);
    //cfopts.set_merge_operator_associative("datasets operator", merge_datasets);
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

    vec![cf_hashes, cf_sigs, cf_colors]
}
