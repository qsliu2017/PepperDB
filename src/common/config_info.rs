//! Translated from PostgreSQL src/include/common/config_info.h

/// One name/setting pair of pg_config output.
pub struct ConfigData {
    pub name: String,
    pub setting: String,
}

/// Collect pg_config data for the given executable path.
pub fn get_configdata(my_exec_path: &str) -> Vec<ConfigData> {
    let _ = my_exec_path;
    unimplemented!()
}
