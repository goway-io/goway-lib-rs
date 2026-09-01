use serde::de::DeserializeOwned;

pub fn load<T: DeserializeOwned>() -> T {
    dotenvy::dotenv().ok();
    match envy::from_env::<T>() {
        Ok(settings) => settings,
        Err(err) => panic!("invalid settings: {err}"),
    }
}
