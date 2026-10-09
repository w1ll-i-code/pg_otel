use std::{ffi::CStr, fmt::Display};

use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting, PostgresGucEnum, log};

const OTLP_ENDPOINT_GUC: &CStr = c"pg_otel.otlp_endpoint";
pub static OTLP_ENDPOINT: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"http://localhost:4317"));

fn define_otlp_endpoint_guc() {
    GucRegistry::define_string_guc(
        OTLP_ENDPOINT_GUC,
        c"OTLP exporter endpoint",
        c"The endpoint used by the OTLP exporter.",
        &OTLP_ENDPOINT,
        GucContext::Sighup,
        GucFlags::default(),
    );
}

fn get_otlp_endpoint() -> Option<String> {
    let Some(guc_var) = OTLP_ENDPOINT.get() else {
        log_config_not_set(OTLP_ENDPOINT_GUC);
        return None;
    };
    match guc_var.into_string() {
        Ok(endpoint) => Some(endpoint),
        Err(err) => {
            log_config_not_valid(OTLP_ENDPOINT_GUC, err);
            None
        }
    }
}

const OTLP_TIMEOUT_MS_GUC: &CStr = c"pg_otel.otlp_timeout_ms";
pub static OTLP_TIMEOUT_MS: GucSetting<i32> = GucSetting::<i32>::new(10_000);

fn define_otlp_timeout_ms_guc() {
    GucRegistry::define_int_guc(
        OTLP_TIMEOUT_MS_GUC,
        c"OTLP exporter timeout in milliseconds",
        c"The timeout used by the OTLP exporter.",
        &OTLP_TIMEOUT_MS,
        1,
        86_400_000,
        GucContext::Sighup,
        GucFlags::default(),
    );
}

const OTLP_PROTOCOL_GUC: &CStr = c"pg_otel.otlp_protocol";
pub static OTLP_PROTOCOL: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"grpc"));

fn define_otlp_protocol_guc() {
    GucRegistry::define_string_guc(
        OTLP_PROTOCOL_GUC,
        c"OTLP protocol",
        c"The protocol used by the OTLP exporter.",
        &OTLP_PROTOCOL,
        GucContext::Sighup,
        GucFlags::default(),
    );
}

fn get_otlp_protocol() -> Option<String> {
    let Some(guc_var) = OTLP_PROTOCOL.get() else {
        log_config_not_set(OTLP_PROTOCOL_GUC);
        return None;
    };
    match guc_var.into_string() {
        Ok(protocol) => Some(protocol),
        Err(err) => {
            log_config_not_valid(OTLP_PROTOCOL_GUC, err);
            None
        }
    }
}

const OTLP_AUTHORIZATION_GUC: &CStr = c"pg_otel.otlp_authorization";
pub static OTLP_AUTHORIZATION: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(None);

fn define_otlp_authorization_guc() {
    GucRegistry::define_string_guc(
        OTLP_AUTHORIZATION_GUC,
        c"OTLP authorization header",
        c"The value of the Authorization header sent to the OTLP collector.",
        &OTLP_AUTHORIZATION,
        GucContext::Sighup,
        // The value is a credential: hide it from SHOW / pg_settings for roles
        // that are neither superusers nor members of pg_read_all_settings.
        GucFlags::SUPERUSER_ONLY | GucFlags::NO_SHOW_ALL,
    );
}

fn get_otlp_authorization() -> Option<String> {
    let guc_var = OTLP_AUTHORIZATION.get()?;
    match guc_var.into_string() {
        Ok(authorization) => Some(authorization),
        Err(err) => {
            log_config_not_valid(OTLP_AUTHORIZATION_GUC, err);
            None
        }
    }
}

const OTLP_CA_CERTIFICATE_GUC: &CStr = c"pg_otel.otlp_ca_certificate";
pub static OTLP_CA_CERTIFICATE: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(None);

fn define_otlp_ca_certificate_guc() {
    GucRegistry::define_string_guc(
        OTLP_CA_CERTIFICATE_GUC,
        c"OTLP CA certificate path",
        c"The path to the CA certificate for the OTLP collector.",
        &OTLP_CA_CERTIFICATE,
        GucContext::Sighup,
        GucFlags::default(),
    );
}

fn get_otlp_ca_certificate() -> Option<String> {
    let guc_var = OTLP_CA_CERTIFICATE.get()?;
    match guc_var.into_string() {
        Ok(cert) => Some(cert),
        Err(err) => {
            log_config_not_valid(OTLP_CA_CERTIFICATE_GUC, err);
            None
        }
    }
}

const OTLP_SERVICE_NAME_GUC: &CStr = c"pg_otel.service_name";
const OTLP_SERVICE_NAME_DEFAULT: &CStr = c"postgresql";
pub static OTLP_SERVICE_NAME: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(OTLP_SERVICE_NAME_DEFAULT));

fn define_otlp_service_name_guc() {
    GucRegistry::define_string_guc(
        OTLP_SERVICE_NAME_GUC,
        c"OTLP service name",
        c"The name of the OTLP service.",
        &OTLP_SERVICE_NAME,
        GucContext::Sighup,
        GucFlags::default(),
    );
}

fn get_otlp_service_name() -> String {
    let Some(guc_var) = OTLP_SERVICE_NAME.get() else {
        return OTLP_SERVICE_NAME_DEFAULT
            .to_str()
            .expect("otlp service name to be valid UTF-8")
            .to_owned();
    };

    match guc_var.into_string() {
        Ok(name) => name,
        Err(err) => {
            log_config_not_valid(OTLP_SERVICE_NAME_GUC, err);
            OTLP_SERVICE_NAME_DEFAULT
                .to_str()
                .expect("otlp service name to be valid UTF-8")
                .to_owned()
        }
    }
}

const OTLP_TRACEPARENT_GUC: &CStr = c"pg_otel.traceparent";
static OTLP_TRACEPARENT: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(None);

fn define_otlp_traceparent_guc() {
    GucRegistry::define_string_guc(
        OTLP_TRACEPARENT_GUC,
        c"OTLP traceparent",
        c"The traceparent header value to use for OTLP traces.",
        &OTLP_TRACEPARENT,
        GucContext::Userset,
        GucFlags::DISALLOW_IN_FILE,
    );
}

/// Returns the raw value of the user-settable `pg_otel.traceparent` GUC, if set.
///
/// The value is not validated here: callers must parse it and treat an invalid
/// one as absent. Use `SET LOCAL` so it cannot leak into later transactions of
/// a pooled connection.
pub fn get_otlp_traceparent() -> Option<String> {
    let guc_var = OTLP_TRACEPARENT.get()?;

    match guc_var.into_string() {
        Ok(traceparent) => Some(traceparent),
        Err(err) => {
            log_config_not_valid(OTLP_TRACEPARENT_GUC, err);
            None
        }
    }
}

/// How much of a statement's text is attached to exported spans.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PostgresGucEnum)]
pub enum QueryTextMode {
    /// Never export query text.
    #[name = c"off"]
    Off,
    /// Export query text with literals replaced by placeholders.
    #[name = c"normalized"]
    Normalized,
    /// Export the query text exactly as received. May contain sensitive data.
    #[name = c"raw"]
    Raw,
}

const QUERY_TEXT_GUC: &CStr = c"pg_otel.query_text";
static QUERY_TEXT: GucSetting<QueryTextMode> =
    GucSetting::<QueryTextMode>::new(QueryTextMode::Normalized);

fn define_query_text_guc() {
    GucRegistry::define_enum_guc(
        QUERY_TEXT_GUC,
        c"Controls query text exported in spans",
        c"One of off, normalized (literals removed) or raw (query text as received).",
        &QUERY_TEXT,
        GucContext::Suset,
        GucFlags::default(),
    );
}

/// Returns the configured [`QueryTextMode`].
pub fn get_query_text_mode() -> QueryTextMode {
    QUERY_TEXT.get()
}

const MIN_DURATION_MS_GUC: &CStr = c"pg_otel.min_duration_ms";
static MIN_DURATION_MS: GucSetting<i32> = GucSetting::<i32>::new(-1);

fn define_min_duration_ms_guc() {
    GucRegistry::define_int_guc(
        MIN_DURATION_MS_GUC,
        c"Minimum statement duration in milliseconds to export a span",
        c"-1 disables exporting, 0 exports every statement.",
        &MIN_DURATION_MS,
        -1,
        i32::MAX,
        GucContext::Suset,
        GucFlags::UNIT_MS,
    );
}

/// Returns the minimum statement duration (ms) for exporting a span.
///
/// `-1` means tracing is disabled and `0` means every statement is traced.
pub fn get_min_duration_ms() -> i32 {
    MIN_DURATION_MS.get()
}

const QUEUE_SIZE_KB_GUC: &CStr = c"pg_otel.queue_size_kb";
static QUEUE_SIZE_KB: GucSetting<i32> = GucSetting::<i32>::new(1024);

fn define_queue_size_kb_guc() {
    GucRegistry::define_int_guc(
        QUEUE_SIZE_KB_GUC,
        c"Size of the shared span queue in kilobytes",
        c"Memory reserved at server start for spans waiting to be exported.",
        &QUEUE_SIZE_KB,
        64,
        1_048_576,
        // Shared memory is sized at postmaster start, so this cannot change later.
        GucContext::Postmaster,
        GucFlags::UNIT_KB,
    );
}

/// Returns the shared span queue size in kilobytes.
#[allow(dead_code)] // wired in later phase
pub fn get_queue_size_kb() -> i32 {
    QUEUE_SIZE_KB.get()
}

#[derive(Clone, Debug)]
pub struct ExporterConfig {
    pub endpoint: String,
    pub protocol: String,
    pub timeout_ms: u32,
    pub authorization: Option<String>,
    pub ca_certificate: Option<String>,
    pub service_name: String,
}

impl ExporterConfig {
    pub fn define_gucs() {
        define_otlp_endpoint_guc();
        define_otlp_protocol_guc();
        define_otlp_timeout_ms_guc();
        define_otlp_authorization_guc();
        define_otlp_ca_certificate_guc();
        define_otlp_service_name_guc();
        define_otlp_traceparent_guc();
        define_query_text_guc();
        define_min_duration_ms_guc();
        define_queue_size_kb_guc();
    }

    pub fn load() -> Option<Self> {
        Some(Self {
            endpoint: get_otlp_endpoint()?,
            protocol: get_otlp_protocol()?,
            timeout_ms: OTLP_TIMEOUT_MS.get() as u32,
            authorization: get_otlp_authorization(),
            ca_certificate: get_otlp_ca_certificate(),
            service_name: get_otlp_service_name(),
        })
    }
}

fn log_config_not_set(name: &CStr) {
    log!("Config variable {:?} is not set", name);
}

fn log_config_not_valid<E: Display>(name: &CStr, error: E) {
    log!("Config variable {:?} is not valid: {}", name, error);
}
