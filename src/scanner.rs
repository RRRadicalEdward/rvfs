use std::path::Path;

use clamav_rs::{
    db,
    engine::{Engine, ScanResult},
    scan_settings::{ScanSettings, ScanSettingsBuilder},
};

pub struct ClamAV {
    engine: Engine,
    settings: ScanSettings,
}

impl ClamAV {
    pub fn new() -> anyhow::Result<Self> {
        clamav_rs::initialize().map_err(Into::<anyhow::Error>::into)?;

        let engine = Engine::new();
        //engine.load_databases(&db::default_directory())?;
        //engine.compile()?;

        let settings = ScanSettingsBuilder::new()
            .enable_archive()
            .enable_mail()
            .enable_ole2()
            .block_broken_executables()
            .enable_phishing_blockssl()
            .enable_phishing_blockcloak()
            .enable_elf()
            .enable_pdf()
            .enable_structured()
            .enable_structured_ssn_normal()
            .enable_structured_ssn_stripped()
            .enable_partial_message()
            .enable_heuristic_precedence()
            .block_macros()
            .enable_structured()
            .enable_xmldocs()
            .enable_hwp3()
            .build();

        Ok(Self { engine, settings })
    }

    pub fn scan(&mut self, path: &Path) -> anyhow::Result<ScanResult> {
        self.engine
            .scan_file(path.as_os_str().to_str().unwrap(), &mut self.settings)
            .map_err(Into::into)
    }
}
