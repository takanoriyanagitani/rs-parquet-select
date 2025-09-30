use std::collections::BTreeSet;
use std::io;
use std::path::Path;
use std::sync::Arc;

use io::Write;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use arrow::record_batch::RecordBatchReader;

use arrow::datatypes::Fields;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;

use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use parquet::file::properties::WriterProperties;

pub fn sch2selected(sch: SchemaRef, cols: &BTreeSet<String>) -> Schema {
    let filtered: Vec<_> = sch
        .fields
        .iter()
        .filter(|field| {
            let name: &str = field.name();
            let found: bool = cols.contains(name);
            found
        })
        .cloned()
        .collect();
    Schema::new(filtered)
}

pub fn batch2selected<I>(
    selected_sch: SchemaRef,
    mut batch: I,
) -> impl Iterator<Item = Result<RecordBatch, io::Error>>
where
    I: Iterator<Item = Result<RecordBatch, io::Error>>,
{
    std::iter::from_fn(move || {
        let orbat: Option<_> = batch.next();
        orbat.map(|rbat: Result<_, _>| {
            rbat.and_then(|bat: RecordBatch| {
                let fields: &Fields = &selected_sch.fields;
                let colsz: usize = fields.len();
                fields
                    .iter()
                    .try_fold(Vec::with_capacity(colsz), |mut state, next| {
                        let colname: &str = next.name();
                        let oacol: Option<&Arc<dyn Array>> = bat.column_by_name(colname);
                        let acol: &Arc<_> =
                            oacol.ok_or(io::Error::other("the specified column missing"))?;
                        state.push(acol.clone());
                        Ok(state)
                    })
                    .and_then(|arr| {
                        RecordBatch::try_new(selected_sch.clone(), arr).map_err(io::Error::other)
                    })
            })
        })
    })
}

pub fn selected2parquet<I, W>(batch: I, mut wtr: ArrowWriter<W>) -> Result<W, io::Error>
where
    W: Write + Send,
    I: Iterator<Item = Result<RecordBatch, io::Error>>,
{
    for rbat in batch {
        let bat: RecordBatch = rbat?;
        wtr.write(&bat)?;
    }
    wtr.into_inner().map_err(io::Error::other)
}

pub fn fsync_nop(_: &mut std::fs::File) -> Result<(), io::Error> {
    Ok(())
}

pub fn fsync_dat(f: &mut std::fs::File) -> Result<(), io::Error> {
    f.sync_data()
}

pub fn fsync_all(f: &mut std::fs::File) -> Result<(), io::Error> {
    f.sync_all()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncType {
    /// No fsync.
    Nop,

    /// fdatasync(Data only).
    Dat,

    /// fsync.
    All,
}

impl FsyncType {
    pub fn to_fn(&self) -> fn(&mut std::fs::File) -> Result<(), io::Error> {
        match self {
            Self::Nop => fsync_nop,
            Self::Dat => fsync_dat,
            Self::All => fsync_all,
        }
    }
}

impl Default for FsyncType {
    fn default() -> Self {
        Self::Nop
    }
}

impl std::str::FromStr for FsyncType {
    type Err = io::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "nop" | "nope" | "none" | "nofsync" => Ok(Self::Nop),
            "dat" | "data" | "fdatasync" => Ok(Self::Dat),
            "all" | "fsync" => Ok(Self::All),
            _ => Err(io::Error::other("invalid fsync type")),
        }
    }
}

pub fn parquet2selected<F>(
    input: std::fs::File,
    cols: &BTreeSet<String>,
    props: Option<WriterProperties>,
    output: std::fs::File,
    fsync: F,
) -> Result<(), io::Error>
where
    F: Fn(&mut std::fs::File) -> Result<(), io::Error>,
{
    let bldr = ParquetRecordBatchReaderBuilder::try_new(input)?;
    let rdr = bldr.build()?;
    let sch: SchemaRef = RecordBatchReader::schema(&rdr);
    let selected: Schema = sch2selected(sch, cols);
    let mapd = rdr.map(|rbat| rbat.map_err(io::Error::other));
    let neo: SchemaRef = selected.into();
    let neo_batch = batch2selected(neo.clone(), mapd);

    let bw = std::io::BufWriter::new(output);
    let wtr = ArrowWriter::try_new(bw, neo, props)?;
    let mut wrote = selected2parquet(neo_batch, wtr)?;
    wrote.flush()?;
    let mut iwtr = wrote.into_inner()?;
    iwtr.flush()?;
    fsync(&mut iwtr)?;
    Ok(())
}

pub fn select_parquet<P>(
    input_parquet: P,
    columns: &BTreeSet<String>,
    props: Option<WriterProperties>,
    output_parquet: P,
    fsync_type: FsyncType,
) -> Result<(), io::Error>
where
    P: AsRef<Path>,
{
    let i = std::fs::File::open(input_parquet)?;
    let o = std::fs::File::create(output_parquet)?;
    let f = fsync_type.to_fn();
    parquet2selected(i, columns, props, o, f)
}
