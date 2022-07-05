use std::collections::HashSet;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use byteorder::{LittleEndian, WriteBytesExt};
use clap::{Parser, Subcommand};
use log::info;
use rayon::prelude::*;
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
    threshold: f64,
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
        // FIXME threshold is f64
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

fn index<P: AsRef<Path>>(
    siglist: P,
    template: Sketch,
    threshold: f64,
    output: P,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Loading siglist");
    let index_sigs = read_paths(siglist)?;
    info!("Loaded {} sig paths in siglist", index_sigs.len());

    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_merge_operator_associative("datasets operator", merge_datasets);
    //opts.set_compaction_style(DBCompactionStyle::Universal);
    //opts.set_min_write_buffer_number_to_merge(10);
    {
        let db = Arc::new(DB::open(&opts, output.as_ref()).unwrap());

        let processed_sigs = AtomicUsize::new(0);
        let sig_iter = index_sigs.par_iter();
        //let sig_iter = index_sigs.iter();

        let _filtered_sigs = sig_iter
            .enumerate()
            .filter_map(|(dataset_id, filename)| {
                let i = processed_sigs.fetch_add(1, Ordering::SeqCst);
                if i % 1000 == 0 {
                    info!("Processed {} reference sigs", i);
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

        info!("Processed {} reference sigs", processed_sigs.into_inner());

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
    }
    //let _ = DB::destroy(&opts, n);
    Ok(())
}

fn check<P: AsRef<Path>>(output: P) -> Result<(), Box<dyn std::error::Error>> {
    use byteorder::ReadBytesExt;

    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_merge_operator_associative("datasets operator", merge_datasets);
    let db = Arc::new(DB::open_for_read_only(&opts, output.as_ref(), true)?);

    let iter = db.iterator(rocksdb::IteratorMode::Start);
    let mut kcount = 0;
    let mut vcount = 0;
    for (key, value) in iter {
        let _k = (&key[..]).read_u64::<LittleEndian>()?;
        kcount += key.len();
        //println!("Saw {} {:?}", k, Datasets::from_slice(&value));
        let _v = Datasets::from_slice(&value).expect("Error with value");
        vcount += value.len();
        //println!("Saw {} {:?}", k, value);
    }

    use size::Size;
    let ksize = Size::from_bytes(kcount);
    let vsize = Size::from_bytes(vcount);
    info!("k: {}, v: {}", ksize.to_string(), vsize.to_string());

    Ok(())
}

fn read_paths<P: AsRef<Path>>(paths_file: P) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let paths = BufReader::new(File::open(paths_file)?);
    Ok(paths
        .lines()
        .map(|line| {
            let mut path = PathBuf::new();
            path.push(line.unwrap());
            path
        })
        .collect())
}

fn build_template(ksize: u8, scaled: usize) -> Sketch {
    let max_hash = max_hash_for_scaled(scaled as u64);
    let template_mh = KmerMinHash::builder()
        .num(0u32)
        .ksize(ksize as u32)
        .max_hash(max_hash)
        .build();
    Sketch::MinHash(template_mh)
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Index {
        /// List of signatures to search
        #[clap(parse(from_os_str))]
        siglist: PathBuf,

        /// ksize
        #[clap(short, long, default_value = "31")]
        ksize: u8,

        /// threshold
        #[clap(short, long, default_value = "0.85")]
        threshold: f64,

        /// scaled
        #[clap(short, long, default_value = "1000")]
        scaled: usize,

        /// The path for output
        #[clap(parse(from_os_str), short, long)]
        output: PathBuf,
    },
    Check {
        /// The path for output
        #[clap(parse(from_os_str), short, long)]
        output: PathBuf,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    use Commands::*;

    let opts = Cli::parse();

    match opts.command {
        Index {
            output,
            siglist,
            threshold,
            ksize,
            scaled,
        } => {
            let template = build_template(ksize, scaled);

            index(siglist, template, threshold, output)?
        }
        Check { output } => check(output)?,
    };

    Ok(())
}
