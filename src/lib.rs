use std::collections::HashSet;
use std::io::Read;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::str;

use byteorder::{BigEndian, ReadBytesExt};
use failure::{err_msg, Error, ResultExt};

include!(concat!(env!("OUT_DIR"), "/capabilities.codegen.rs"));

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Capability {
    RunCommand,
    GetEncoding,
    Unknown(String),
}

impl<S> From<S> for Capability
where
    S: AsRef<str> + Into<String>,
{
    fn from(s: S) -> Self {
        CAPABILITIES
            .get(s.as_ref())
            .cloned()
            .unwrap_or_else(|| Capability::Unknown(s.into()))
    }
}

#[derive(Debug)]
pub struct Connection {
    hg: Child,
    encoding: String,
    capabilities: HashSet<Capability>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.hg.kill().unwrap_or(())
    }
}

impl Connection {
    pub fn new() -> Result<Connection, Error> {
        let mut hg = Self::start_hg(Path::new("hg"))?;

        let chunk = {
            let stdout = hg.stdout.as_mut().expect("no stdout handle");
            Self::read_chunk_from(stdout).context("Could not read hello")?
        };

        let Chunk::Output(bytes) = chunk;
        let s = str::from_utf8(&bytes)?;

        let mut encoding = None;
        let mut capabilities: Option<HashSet<Capability>> = None;

        for line in s.lines() {
            let split_at = line
                .find(": ")
                .ok_or_else(|| err_msg("no field: value in hello"))?;
            let (key, value) = line.split_at(split_at);
            let value = &value[2..];

            match key {
                "capabilities" => {
                    capabilities = Some(value.split(" ").map(Into::into).collect());
                }
                "encoding" => encoding = Some(value.into()),
                _ => {
                    // Ignore unknown fields in hello.
                }
            }
        }

        let encoding = encoding.ok_or_else(|| err_msg("no encoding in hello"))?;
        let capabilities = capabilities.ok_or_else(|| err_msg("no capabilities in hello"))?;

        if !capabilities.contains(&Capability::RunCommand) {
            Err(err_msg("No runcommand capability"))
        } else {
            Ok(Connection {
                hg,
                encoding,
                capabilities,
            })
        }
    }

    pub fn capabilities(&self) -> &HashSet<Capability> {
        &self.capabilities
    }

    pub fn encoding(&self) -> &str {
        &self.encoding
    }

    fn start_hg(hg: &Path) -> Result<Child, Error> {
        Command::new(hg)
            .arg("serve")
            .arg("--cmdserver")
            .arg("pipe")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .context("Could not launch hg")
            .map_err(Into::into)
    }

    fn read_chunk_from<R>(r: &mut R) -> Result<Chunk, Error>
    where
        R: Read,
    {
        let channel = {
            let mut buf = [0u8];
            r.read_exact(&mut buf)?;

            buf[0]
        };

        let chunk = match channel {
            b'o' => {
                let len = r.read_u32::<BigEndian>()? as usize;
                let mut buf = vec![0; len];

                r.read_exact(&mut buf)?;

                Chunk::Output(buf)
            }

            b'e' => unimplemented!(),
            b'r' => unimplemented!(),
            b'd' => unimplemented!(),
            b'I' => unimplemented!(),
            b'L' => unimplemented!(),

            c => panic!("Unknown channel: {:?}", c),
        };

        Ok(chunk)
    }

    pub fn read_chunk(&mut self) -> Result<Chunk, Error> {
        Self::read_chunk_from(self.hg.stdout.as_mut().expect("no stdout handle"))
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Chunk {
    Output(Vec<u8>),
}

#[cfg(test)]
mod test {
    use crate::{Capability, Connection};

    #[test]
    fn hello() -> Result<(), failure::Error> {
        let conn = Connection::new()?;
        assert_eq!(conn.encoding(), "UTF-8");
        assert!(conn.capabilities().contains(&Capability::RunCommand));

        Ok(())
    }
}
