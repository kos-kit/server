use anyhow::{self, bail};
use flate2::read::MultiGzDecoder;
use oxigraph::io::{DatasetFormat, GraphFormat};
use oxigraph::model::GraphNameRef;
use oxigraph::store::{BulkLoader, Store};
use rayon_core::ThreadPoolBuilder;
use std::cmp::max;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::thread::available_parallelism;
use std::time::Instant;

#[derive(Copy, Clone)]
enum GraphOrDatasetFormat {
    Graph(GraphFormat),
    Dataset(DatasetFormat),
}

impl GraphOrDatasetFormat {
    fn from_path(path: &Path) -> anyhow::Result<Self> {
        format_from_path(path, Self::from_extension)
    }

    fn from_extension(name: &str) -> anyhow::Result<Self> {
        Ok(match (GraphFormat::from_extension(name), DatasetFormat::from_extension(name)) {
            (Some(g), Some(d)) => bail!("The file extension '{name}' can be resolved to both '{}' and '{}', not sure what to pick", g.file_extension(), d.file_extension()),
            (Some(g), None) => Self::Graph(g),
            (None, Some(d)) => Self::Dataset(d),
            (None, None) =>
            bail!("The file extension '{name}' is unknown")
        })
    }
}

fn format_from_path<T>(
    path: &Path,
    from_extension: impl FnOnce(&str) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    if let Some(ext) = path.extension().and_then(OsStr::to_str) {
        from_extension(ext).map_err(|e| {
            e.context(format!(
                "Not able to guess the file format from file name extension '{ext}'"
            ))
        })
    } else {
        bail!(
            "The path {} has no extension to guess a file format from",
            path.display()
        )
    }
}

fn bulk_load_oxigraph(
    loader: &BulkLoader,
    reader: impl BufRead,
    format: GraphOrDatasetFormat,
) -> anyhow::Result<()> {
    match format {
        GraphOrDatasetFormat::Graph(format) => {
            loader.load_graph(reader, format, GraphNameRef::DefaultGraph, None)?
        }
        GraphOrDatasetFormat::Dataset(format) => loader.load_dataset(reader, format, None)?,
    }
    Ok(())
}

pub fn init(directory_path: PathBuf, store: Store) -> anyhow::Result<()> {
    init_oxigraph(directory_path, store)
}

fn init_oxigraph(init_path: PathBuf, store: Store) -> anyhow::Result<()> {
    let file_paths = if fs::metadata(init_path.clone())?.is_file() {
        vec![init_path]
    } else {
        fs::read_dir(init_path)?
            .filter_map(|res| res.ok())
            .filter(|dir_entry| {
                dir_entry
                    .file_type()
                    .is_ok_and(|file_type| file_type.is_file())
            })
            .map(|dir_entry| dir_entry.path())
            .collect::<Vec<_>>()
    };

    ThreadPoolBuilder::new()
        .num_threads(max(1, available_parallelism()?.get() / 2))
        .thread_name(|i| format!("Oxigraph bulk loader thread {i}"))
        .build()?
        .scope(|s| {
            for file_path in file_paths {
                let store = store.clone();
                s.spawn(move |_| {
                    let f = file_path.clone();
                    let start = Instant::now();
                    let loader = store.bulk_loader().on_progress(move |size| {
                        let elapsed = start.elapsed();
                        eprintln!(
                            "{} triples loaded in {}s ({} t/s) from {}",
                            size,
                            elapsed.as_secs(),
                            ((size as f64) / elapsed.as_secs_f64()).round(),
                            f.display()
                        )
                    });
                    let fp = match File::open(&file_path) {
                        Ok(fp) => fp,
                        Err(error) => {
                            eprintln!(
                                "Error while opening file {}: {}",
                                file_path.display(),
                                error
                            );
                            return;
                        }
                    };
                    if let Err(error) = {
                        if file_path
                            .extension()
                            .map_or(false, |e| e == OsStr::new("gz"))
                        {
                            bulk_load_oxigraph(
                                &loader,
                                BufReader::new(MultiGzDecoder::new(fp)),
                                GraphOrDatasetFormat::from_path(&file_path.with_extension(""))
                                    .unwrap(),
                            )
                        } else {
                            bulk_load_oxigraph(
                                &loader,
                                BufReader::new(fp),
                                GraphOrDatasetFormat::from_path(&file_path).unwrap(),
                            )
                        }
                    } {
                        eprintln!(
                            "Error while loading file {}: {}",
                            file_path.display(),
                            error
                        )
                        //TODO: hard fail
                    }
                })
            }
        });
    store.flush()?;

    Ok(())
}