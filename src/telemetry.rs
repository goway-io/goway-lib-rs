use tracing_subscriber::EnvFilter;

pub fn init(service_name: &str, environment: &str, glitchtip_dsn: Option<&str>) -> Option<sentry::ClientInitGuard> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    let dsn = glitchtip_dsn.filter(|dsn| !dsn.is_empty())?;
    let mut options = sentry::ClientOptions::default();
    options.release = sentry::release_name!();
    options.environment = Some(environment.to_owned().into());
    let guard = sentry::init((dsn, options));
    tracing::info!(service = service_name, "glitchtip reporting enabled");
    Some(guard)
}
