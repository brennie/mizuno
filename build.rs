use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use failure::Error;
use phf_codegen::Map;

fn main() -> Result<(), Error> {
    let path = Path::new(&env::var("OUT_DIR")?).join("capabilities.codegen.rs");
    let mut file = BufWriter::new(File::create(&path)?);

    write!(&mut file, "#[allow(clippy::all)]\n")?;
    write!(
        &mut file,
        "static CAPABILITIES: phf::Map<&'static str, Capability> = "
    )?;

    Map::new()
        .entry("runcommand", "Capability::RunCommand")
        .entry("getencoding", "Capability::GetEncoding")
        .build(&mut file)?;

    write!(&mut file, ";\n")?;

    Ok(())
}
