use std::{
    cmp::min, collections::BTreeMap, fs::File, path::{Path, PathBuf}, ptr::addr_eq
};

use clap::Parser;
use color_eyre::eyre::{Context, Result};
use indicatif::{HumanBytes, ParallelProgressIterator, ProgressIterator as _};
use memmap2::Mmap;
use rayon::prelude::*;
use walkdir::WalkDir;

#[derive(Parser)]
struct Cli {
    root: PathBuf,

    #[clap(long)]
    print_groups: bool,

    #[clap(long)]
    detect_similar_images: bool,
}

type PdqHash = ([u8; 32], f32);

#[derive(Debug)]
struct FileData {
    path: PathBuf,
    file_hash: Option<u64>,
    size: Option<usize>,

    perception_hash: Option<PdqHash>,
}

impl FileData {
    pub fn from_file(path: PathBuf) -> Self {
        Self {
            path,
            file_hash: None,
            size: None,
            perception_hash: None,
        }
    }

    pub fn hash(&mut self, try_perception_hash: bool) -> Result<()> {
        let file = File::open(&self.path)
            .wrap_err_with(|| format!("Trying to open {}", self.path.display()))?;

        let mmap = unsafe {
            Mmap::map(&file)
                .wrap_err_with(|| format!("Failed to memory map {}", self.path.display()))?
        };

        let prefix = min(mmap.len(), 4096);
        self.file_hash = Some(seahash::hash(&mmap[0..prefix]));
        self.size = Some(mmap.len());

        if try_perception_hash {
            self.perception_hash = (||{
                let img = pdqhash::image::load_from_memory(&mmap).ok()?;
                pdqhash::generate_pdq(&img)
            })();
        }

        Ok(())
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    color_eyre::install()?;

    let data = collect(&cli.root);

    println!("Found {} files", data.len());


    println!("Calculating hashes...");
    let data: Vec<_> = data
        .into_par_iter()
        .progress()
        .filter_map(|file| {
            let result = (move || -> Result<_>{
                let mut file = file?;
                file.hash(cli.detect_similar_images)?;
                Ok(file)
            })();

            match result {
                Ok(file) => Some(file),
                Err(err) => {
                    println!("Failed to hash file: {err}");
                    None
                },
            }
        })
        .collect();

    let num_files = data.len();
    let total_size: usize = data.iter().map(|file| file.size.unwrap()).sum();

    println!("Hashed {} files ({})", num_files, HumanBytes(total_size as u64));


    if cli.detect_similar_images {
        build_perception_groups(&data, &cli);
    } else {
        build_exact_groups(&data, &cli);
    }


    Ok(())
}

fn build_exact_groups(data: &[FileData], cli: &Cli) {
    let mut groups = group_candates(data);

    groups.retain(|_, v| v.len() > 1);

    println!("Got {} possible duplicates", groups.len());

    let avg = groups.iter().map(|(_, v)| v.len()).sum::<usize>()/groups.len();
    println!("On average {avg} elements per group");

    if cli.print_groups {
        for (hash, files) in &groups {
            println!("=== {hash} ===");
            for file in files {
                println!("{}", file.path.display());
            }
            println!();
        }
    }
}

fn build_perception_groups(data: &[FileData], cli: &Cli)  {
    const ALLOWED_DISTANCE: u64 = 3;

    let images: Vec<_> = data.iter().filter(|o| o.perception_hash.is_some()).collect();

    println!("Found {} images in dataset", images.len());

    let mut groups = Vec::new();

    for &image in images.iter().progress() {
        let self_hash = image.perception_hash.unwrap();

        let similars: Vec<_> = images.iter().filter(|&&other| {
            if addr_eq(image, other) {
                return false;
            }

            let other_hash = other.perception_hash.unwrap();

            hamming::distance(&self_hash.0, &other_hash.0) <= ALLOWED_DISTANCE
        }).collect();

        if !similars.is_empty() {
            groups.push((image, similars));
        }
    }

    for (image, similars) in groups {
        println!("Found {} images similar to {}", similars.len(), image.path.display());

        if cli.print_groups {
            for file in similars {
                println!("{}", file.path.display());
            }
            println!();
        }
    }
}

fn group_candates<'a>(items: impl IntoIterator<Item=&'a FileData>) -> BTreeMap<u64, Vec<&'a FileData>> {
    let mut map: BTreeMap<u64, Vec<&'a FileData>> = BTreeMap::new();

    for item in items {
        map.entry(item.file_hash.unwrap()).or_default().push(item);
    }

    map
}

fn collect(path: &Path) -> Vec<Result<FileData>> {
    WalkDir::new(path)
        .into_iter()
        .par_bridge()
        .filter_map(|elem| {
            let elem = match elem {
                Ok(e) => e,
                Err(err) => {
                    return Some(
                        Err(err)
                            .wrap_err_with(|| format!("Invalid directory entry while iterating!")),
                    )
                }
            };

            let path = elem.path();

            if !elem.path().is_file() {
                return None;
            }

            Some(Ok(FileData::from_file(path.to_owned())))
        })
        .collect()
}
