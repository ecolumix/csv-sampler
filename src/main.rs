/// src/main.rs
///
use anyhow::Result;
use clap::Parser;
use polars::prelude::*;
use std::fs::File;
use time::Instant;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    file: String,

    #[arg(short, long, default_value_t = 10.0)]
    percent: f64,

    #[arg(short, long, default_value_t = 10_000)]
    max_records: i32, // Number of records for polars to scan to determine a column's type

    #[arg(short, long)]
    outfile: String,

    #[arg(short, long)]
    seed: Option<u64>,
}

fn main() -> Result<()> {
    let t1 = Instant::now();

    let args = Args::parse();

    let percent = args.percent / 100.0;

    let input_path = args.file;

    let max_records = Some(args.max_records as usize);

    // Read the CSV file into a DataFrame
    //    let file = File::open(input_path)?;

    let df = CsvReader::from_path(input_path)?
        .infer_schema(max_records)
        .has_header(true)
        .finish()?;

    // Sample a percentage of rows
    let n = ((percent * df.shape().0 as f64).floor()) as usize;

    let mut sampled_df = df.sample_n_literal(n, false, false, args.seed)?;

    let mut outfile = File::create(&args.outfile).expect("Could not create output file...");

    let _ = CsvWriter::new(&mut outfile)
        .has_header(true)
        .with_separator(b',')
        .finish(&mut sampled_df);

    let t2 = Instant::now();

    println!("Sampling completed in: {:?}", t2 - t1);

    Ok(())
}
