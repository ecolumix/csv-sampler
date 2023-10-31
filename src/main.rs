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
    percent: f32,

    #[arg(short, long, default_value_t = 10_000)]
    max_records: i32,
}

fn main() -> Result<()> {
    let t1 = Instant::now();

    let args = Args::parse();

    let percent = args.percent / 100.0;

    let input_path = args.file;

    let max_records = Some(args.max_records as usize);

    // Read the CSV file into a DataFrame
    let file = File::open(input_path)?;

    let df = CsvReader::new(file)
        .infer_schema(max_records)
        .has_header(true)
        .finish()?;

    // Sample a percentage of rows
    let n = ((percent * df.shape().0 as f32).floor()) as usize;

    let sampled_df = df.sample_n_literal(n, false, false, None)?;

    let t2 = Instant::now();

    println!("Sampling completed in: {:?}", t2 - t1);

    // Print the sampled DataFrame
    println!("{:?}", sampled_df);

    Ok(())
}
