use std::{ffi::CStr, fmt::Display};

use pgrx::{log, GucContext, GucFlags, GucRegistry, GucSetting};

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
        GucFlags::default(),
    );
}

fn get_otlp_authorization() -> Option<String> {
    let Some(guc_var) = OTLP_AUTHORIZATION.get() else {
        return None;
    };
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
    let Some(guc_var) = OTLP_CA_CERTIFICATE.get() else {
        return None;
    };
    match guc_var.into_string() {
        Ok(cert) => Some(cert),
        Err(err) => {
            log_config_not_valid(OTLP_AUTHORIZATION_GUC, err);
            None
        }
    }
}

const OTLP_SERVICE_NAME_GUC: &CStr = c"otel.service.name";
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

const OTLP_TRACEPARENT_GUC: &CStr = c"otel.traceparent";
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

pub fn get_otlp_traceparent() -> Option<String> {
    let guc_var = OTLP_SERVICE_NAME.get()?;

    match guc_var.into_string() {
        Ok(name) => Some(name),
        Err(err) => {
            log_config_not_valid(OTLP_SERVICE_NAME_GUC, err);
            None
        }
    }
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
