use std::collections::BTreeSet;
use std::io;
use std::process::ExitCode;

use rs_parquet_select::sync::FsyncType;
use rs_parquet_select::sync::select_parquet;

fn env2input_filename() -> Result<String, io::Error> {
    std::env::var("ENV_INPUT_PARQUET_FILENAME").map_err(io::Error::other)
}

fn env2output_filename() -> Result<String, io::Error> {
    std::env::var("ENV_OUTPUT_PARQUET_FILENAME").map_err(io::Error::other)
}

fn env2column_names() -> Result<Vec<String>, io::Error> {
    std::env::var("ENV_COLUMN_NAMES")
        .map_err(io::Error::other)
        .map(|s| s.as_str().split(',').map(|s| s.into()).collect())
}

fn sub() -> Result<(), io::Error> {
    let iname: String = env2input_filename()?;
    let oname: String = env2output_filename()?;
    let columns: Vec<String> = env2column_names()?;

    select_parquet(
        iname,
        &BTreeSet::from_iter(columns),
        None,
        oname,
        FsyncType::default(),
    )?;
    Ok(())
}

fn main() -> ExitCode {
    match sub() {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
