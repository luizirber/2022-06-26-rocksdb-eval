use std::path::PathBuf;

use clap::{Parser, Subcommand};

use sourmash::sketch::minhash::{max_hash_for_scaled, KmerMinHash};
use sourmash::sketch::Sketch;

use rocks_eval::{check, index, search};

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
        #[clap(short, long, default_value = "0")]
        threshold: f64,

        /// scaled
        #[clap(short, long, default_value = "1000")]
        scaled: usize,

        /// save paths to signatures into index. Default: save full sig into index
        #[clap(long)]
        save_paths: bool,

        /// The path for output
        #[clap(parse(from_os_str), short, long)]
        output: PathBuf,

        /// Index using colors
        #[clap(long = "colors")]
        colors: bool,
    },
    Check {
        /// The path for output
        #[clap(parse(from_os_str))]
        output: PathBuf,

        /// avoid deserializing data, and without stats
        #[clap(long = "quick")]
        quick: bool,

        /// check using colors
        #[clap(long = "colors")]
        colors: bool,
    },
    Search {
        /// Query signature
        #[clap(parse(from_os_str))]
        query_path: PathBuf,

        /// Path to rocksdb index dir
        #[clap(parse(from_os_str))]
        index: PathBuf,

        /// ksize
        #[clap(short = 'k', long = "ksize", default_value = "31")]
        ksize: u8,

        /// scaled
        #[clap(short = 's', long = "scaled", default_value = "1000")]
        scaled: usize,

        /// threshold_bp
        #[clap(short = 't', long = "threshold_bp", default_value = "50000")]
        threshold_bp: usize,

        /// The path for output
        #[clap(parse(from_os_str), short = 'o', long = "output")]
        output: Option<PathBuf>,

        /// search using colors
        #[clap(long = "colors")]
        colors: bool,
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
            save_paths,
            colors,
        } => {
            let template = build_template(ksize, scaled);

            index(siglist, template, threshold, output, save_paths, colors)?
        }
        Check {
            output,
            quick,
            colors,
        } => check(output, quick, colors)?,
        Search {
            query_path,
            output,
            index,
            threshold_bp,
            ksize,
            scaled,
            colors,
        } => {
            let template = build_template(ksize, scaled);

            search(query_path, index, template, threshold_bp, output, colors)?
        }
    };

    Ok(())
}
