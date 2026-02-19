use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use thiserror::Error;

use crate::cli::{Client, ClientError};
use crate::daemon;
use crate::protocol::VersionResult;

const SYSTEMD_SERVICE_NAME: &str = "ghfs";
const SYSTEMD_UNIT_FILE: &str = "ghfs.service";
const LAUNCHD_LABEL: &str = "com.ghfs.daemon";
const LAUNCHD_PLIST_FILE: &str = "com.ghfs.daemon.plist";
const DEFAULT_SERVICE_PATH: &str = "/usr/local/bin:/usr/bin:/bin:/usr/local/sbin:/usr/sbin:/sbin";

#[derive(Debug, Clone, Copy)]
pub enum ServiceBackend {
    Systemd,
    Launchd,
}

impl ServiceBackend {
    pub fn detect() -> Result<Self, ServiceError> {
        if cfg!(target_os = "macos") {
            Ok(Self::Launchd)
        } else if cfg!(target_os = "linux") {
            if Path::new("/run/systemd/system").exists() {
                Ok(Self::Systemd)
            } else {
                Err(ServiceError::BackendUnavailable(
                    "systemd not found; manual setup required".to_string(),
                ))
            }
        } else {
            Err(ServiceError::UnsupportedPlatform)
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Systemd => "systemd (user)",
            Self::Launchd => "launchd",
        }
    }

    pub fn installed_kind(self) -> &'static str {
        match self {
            Self::Systemd => "systemd user unit",
            Self::Launchd => "launchd agent",
        }
    }

    pub fn file_label(self) -> &'static str {
        match self {
            Self::Systemd => "Unit",
            Self::Launchd => "Plist",
        }
    }

    pub fn manager_name(self) -> &'static str {
        match self {
            Self::Systemd => "systemd",
            Self::Launchd => "launchd",
        }
    }

    pub fn service_file_path(self) -> Result<PathBuf, ServiceError> {
        match self {
            Self::Systemd => systemd_unit_path(),
            Self::Launchd => launchd_plist_path(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServiceInstallStatus {
    pub backend: ServiceBackend,
    pub path: PathBuf,
    pub installed: bool,
    pub nix_managed: bool,
}

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("unsupported platform")]
    UnsupportedPlatform,

    #[error("{0}")]
    BackendUnavailable(String),

    #[error("unable to determine home directory")]
    HomeDirNotFound,

    #[error("service is not installed")]
    NotInstalled,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("client error: {0}")]
    Client(#[from] ClientError),

    #[error("command failed: {command} (exit code: {code:?}){stderr}")]
    CommandFailed {
        command: String,
        code: Option<i32>,
        stderr: String,
    },
}

pub fn install(no_start: bool) -> Result<(), ServiceError> {
    match ServiceBackend::detect()? {
        ServiceBackend::Systemd => install_systemd(no_start),
        ServiceBackend::Launchd => install_launchd(no_start),
    }
}

pub fn uninstall() -> Result<(), ServiceError> {
    match ServiceBackend::detect()? {
        ServiceBackend::Systemd => uninstall_systemd(),
        ServiceBackend::Launchd => uninstall_launchd(),
    }
}

pub fn start() -> Result<(), ServiceError> {
    match ServiceBackend::detect()? {
        ServiceBackend::Systemd => start_systemd(),
        ServiceBackend::Launchd => start_launchd(),
    }
}

pub fn stop(force: bool) -> Result<(), ServiceError> {
    if force {
        kill_open_mount_processes()?;
    }

    match ServiceBackend::detect()? {
        ServiceBackend::Systemd => stop_systemd(),
        ServiceBackend::Launchd => stop_launchd(),
    }
}

pub fn restart() -> Result<(), ServiceError> {
    match ServiceBackend::detect()? {
        ServiceBackend::Systemd => restart_systemd(),
        ServiceBackend::Launchd => restart_launchd(),
    }
}

pub fn logs() -> Result<(), ServiceError> {
    match ServiceBackend::detect()? {
        ServiceBackend::Systemd => logs_systemd(),
        ServiceBackend::Launchd => logs_launchd(),
    }
}

pub fn status() -> Result<(), ServiceError> {
    let install = installation_status()?;

    if !install.installed {
        println!("Service: not installed");
        println!("Backend: {}", install.backend.label());
        println!("Run:     ghfs service install");
        return Ok(());
    }

    let manager_running = match install.backend {
        ServiceBackend::Systemd => systemd_is_active().unwrap_or(false),
        ServiceBackend::Launchd => launchd_runtime_state()
            .map(|(running, _)| running)
            .unwrap_or(false),
    };

    let manager_pid = match install.backend {
        ServiceBackend::Systemd => systemd_main_pid().unwrap_or(None),
        ServiceBackend::Launchd => launchd_runtime_state().ok().and_then(|(_, pid)| pid),
    };

    let daemon = daemon_version().ok();

    if let Some(version) = daemon.as_ref() {
        println!("Service: running (PID {})", version.pid);
    } else if let Some(pid) = manager_pid {
        println!("Service: installed but not running");
        println!("         Run 'ghfs service start' to start it.");
        println!("State:   service manager reports PID {}", pid);
    } else {
        println!("Service: installed but not running");
        println!("         Run 'ghfs service start' to start it.");
        if manager_running {
            println!("State:   service manager reports running");
        }
    }

    println!("Backend: {}", install.backend.label());
    println!(
        "{}:    {}",
        install.backend.file_label(),
        display_home_relative(&install.path)
    );

    if install.nix_managed {
        println!("Managed: Nix");
    }

    if let ServiceBackend::Systemd = install.backend {
        if let Ok(enabled) = systemd_is_enabled() {
            println!("Enabled: {}", if enabled { "yes" } else { "no" });
        }
    }

    if let Some(version) = daemon {
        let cli_version = env!("CARGO_PKG_VERSION");
        if version.version != cli_version {
            println!(
                "Version: {} (daemon) -> {} (cli)",
                version.version, cli_version
            );
            println!(
                "         ! Daemon is outdated. Run 'ghfs service restart' to pick up the new version."
            );
        }
    }

    Ok(())
}

pub fn installation_status() -> Result<ServiceInstallStatus, ServiceError> {
    let backend = ServiceBackend::detect()?;
    let path = backend.service_file_path()?;
    let installed = path.exists();
    let nix_managed = installed && is_nix_managed_service(&path);

    Ok(ServiceInstallStatus {
        backend,
        path,
        installed,
        nix_managed,
    })
}

pub fn kill_open_mount_processes() -> Result<(), ServiceError> {
    let mount_point = daemon::mount_point();
    let target = mount_point.to_string_lossy().to_string();
    let pids = find_open_file_pids(&target);

    if pids.is_empty() {
        return Ok(());
    }

    println!("Killing {} process(es) with open files...", pids.len());
    for pid in pids {
        let mut cmd = Command::new("kill");
        cmd.arg("-9").arg(pid.to_string());
        let _ = cmd.status();
    }

    std::thread::sleep(std::time::Duration::from_millis(100));
    Ok(())
}

fn install_systemd(no_start: bool) -> Result<(), ServiceError> {
    let unit_path = systemd_unit_path()?;
    let existed = unit_path.exists();

    if existed && is_nix_managed_service(&unit_path) {
        print_nix_managed_warning(ServiceBackend::Systemd);
        return Ok(());
    }

    if let Some(parent) = unit_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let exe = std::env::current_exe()?;
    let path_env = service_path_env();
    ensure_linux_fuse_helper_available(&path_env)?;
    let unit_contents = format!(
        "[Unit]\nDescription=GHFS GitHub Filesystem\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nExecStart={} daemon\nRestart=on-failure\nRestartSec=5\nEnvironment=RUST_LOG=info\nEnvironment=\"PATH={}\"\n\n[Install]\nWantedBy=default.target\n",
        exe.display(),
        escape_systemd_value(&path_env),
    );

    fs::write(&unit_path, unit_contents)?;
    run_checked(systemctl_user(["daemon-reload"]))?;

    if existed {
        println!("Updated systemd user service");
        run_checked(systemctl_user(["enable", SYSTEMD_SERVICE_NAME]))?;
        if no_start {
            println!("Service updated but not restarted (--no-start)");
        } else {
            run_checked(systemctl_user(["restart", SYSTEMD_SERVICE_NAME]))?;
            println!("Restarted ghfs daemon");
        }
    } else {
        println!("Installed systemd user service");
        if no_start {
            run_checked(systemctl_user(["enable", SYSTEMD_SERVICE_NAME]))?;
            println!("Service installed but not started");
        } else {
            run_checked(systemctl_user(["enable", "--now", SYSTEMD_SERVICE_NAME]))?;
            println!("Started ghfs daemon");
        }
    }

    println!("Daemon will start automatically on login");
    Ok(())
}

fn uninstall_systemd() -> Result<(), ServiceError> {
    let unit_path = systemd_unit_path()?;

    if !unit_path.exists() {
        println!("Systemd user service is not installed");
        return Ok(());
    }

    if is_nix_managed_service(&unit_path) {
        print_nix_managed_warning(ServiceBackend::Systemd);
        return Ok(());
    }

    let _ = run_ignore_failure(systemctl_user(["disable", "--now", SYSTEMD_SERVICE_NAME]));
    fs::remove_file(&unit_path)?;
    run_checked(systemctl_user(["daemon-reload"]))?;
    println!("Uninstalled systemd user service");
    Ok(())
}

fn start_systemd() -> Result<(), ServiceError> {
    ensure_systemd_installed()?;
    run_checked(systemctl_user(["start", SYSTEMD_SERVICE_NAME]))?;
    println!("Started ghfs daemon");
    Ok(())
}

fn stop_systemd() -> Result<(), ServiceError> {
    ensure_systemd_installed()?;
    run_checked(systemctl_user(["stop", SYSTEMD_SERVICE_NAME]))?;
    println!("Stopped ghfs daemon");
    Ok(())
}

fn restart_systemd() -> Result<(), ServiceError> {
    ensure_systemd_installed()?;
    run_checked(systemctl_user(["restart", SYSTEMD_SERVICE_NAME]))?;
    println!("Restarted ghfs daemon");
    Ok(())
}

fn logs_systemd() -> Result<(), ServiceError> {
    ensure_systemd_installed()?;
    run_interactive(systemctl_journalctl())
}

fn install_launchd(no_start: bool) -> Result<(), ServiceError> {
    let plist_path = launchd_plist_path()?;
    let existed = plist_path.exists();

    if existed && is_nix_managed_service(&plist_path) {
        print_nix_managed_warning(ServiceBackend::Launchd);
        return Ok(());
    }

    if let Some(parent) = plist_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let (stdout_log, stderr_log) = launchd_log_paths()?;
    if let Some(parent) = stdout_log.parent() {
        fs::create_dir_all(parent)?;
    }

    let exe = std::env::current_exe()?;
    let path_env = service_path_env();
    let plist = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\">\n<dict>\n    <key>Label</key>\n    <string>{label}</string>\n    <key>ProgramArguments</key>\n    <array>\n        <string>{exe}</string>\n        <string>daemon</string>\n    </array>\n    <key>RunAtLoad</key>\n    <true/>\n    <key>KeepAlive</key>\n    <true/>\n    <key>StandardOutPath</key>\n    <string>{stdout_log}</string>\n    <key>StandardErrorPath</key>\n    <string>{stderr_log}</string>\n    <key>EnvironmentVariables</key>\n    <dict>\n        <key>RUST_LOG</key>\n        <string>info</string>\n        <key>PATH</key>\n        <string>{path_env}</string>\n    </dict>\n</dict>\n</plist>\n",
        label = LAUNCHD_LABEL,
        exe = xml_escape(&exe.to_string_lossy()),
        stdout_log = xml_escape(&stdout_log.to_string_lossy()),
        stderr_log = xml_escape(&stderr_log.to_string_lossy()),
        path_env = xml_escape(&path_env),
    );

    fs::write(&plist_path, plist)?;

    if existed {
        println!("Updated launchd agent");
        let _ = run_ignore_failure(launchctl_cmd("unload", Some(&plist_path), None));
        if no_start {
            println!("Service updated but not restarted (--no-start)");
        } else {
            run_checked(launchctl_cmd("load", Some(&plist_path), Some("-w")))?;
            println!("Restarted ghfs daemon");
        }
    } else {
        println!("Installed launchd agent");
        if no_start {
            println!("Service installed but not started");
        } else {
            run_checked(launchctl_cmd("load", Some(&plist_path), Some("-w")))?;
            println!("Started ghfs daemon");
        }
    }

    if !no_start {
        println!("Daemon will start automatically on login");
    }

    Ok(())
}

fn uninstall_launchd() -> Result<(), ServiceError> {
    let plist_path = launchd_plist_path()?;

    if !plist_path.exists() {
        println!("Launchd agent is not installed");
        return Ok(());
    }

    if is_nix_managed_service(&plist_path) {
        print_nix_managed_warning(ServiceBackend::Launchd);
        return Ok(());
    }

    let _ = run_ignore_failure(launchctl_cmd("unload", Some(&plist_path), None));
    fs::remove_file(&plist_path)?;
    println!("Uninstalled launchd agent");
    Ok(())
}

fn start_launchd() -> Result<(), ServiceError> {
    let plist_path = launchd_plist_path()?;
    if !plist_path.exists() {
        return Err(ServiceError::NotInstalled);
    }

    if launchctl_list(LAUNCHD_LABEL)?.status.success() {
        run_checked(launchctl_cmd("start", None, Some(LAUNCHD_LABEL)))?;
    } else {
        run_checked(launchctl_cmd("load", Some(&plist_path), Some("-w")))?;
    }

    println!("Started ghfs daemon");
    Ok(())
}

fn stop_launchd() -> Result<(), ServiceError> {
    let plist_path = launchd_plist_path()?;
    if !plist_path.exists() {
        return Err(ServiceError::NotInstalled);
    }

    if !launchctl_list(LAUNCHD_LABEL)?.status.success() {
        println!("ghfs daemon is not running");
        return Ok(());
    }

    run_checked(launchctl_cmd("stop", None, Some(LAUNCHD_LABEL)))?;
    println!("Stopped ghfs daemon");
    Ok(())
}

fn restart_launchd() -> Result<(), ServiceError> {
    let plist_path = launchd_plist_path()?;
    if !plist_path.exists() {
        return Err(ServiceError::NotInstalled);
    }

    if !launchctl_list(LAUNCHD_LABEL)?.status.success() {
        run_checked(launchctl_cmd("load", Some(&plist_path), Some("-w")))?;
        println!("Started ghfs daemon");
        return Ok(());
    }

    run_checked(launchctl_cmd("stop", None, Some(LAUNCHD_LABEL)))?;
    println!("Restarted ghfs daemon");
    Ok(())
}

fn logs_launchd() -> Result<(), ServiceError> {
    let log_path = launchd_log_paths()?.0;
    if let Some(parent) = log_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let _ = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;

    let mut cmd = Command::new("tail");
    cmd.args(["-f"]).arg(log_path);
    run_interactive(cmd)
}

fn daemon_version() -> Result<VersionResult, ServiceError> {
    let mut client = Client::connect()?;
    let version = client.version()?;
    Ok(version)
}

fn ensure_systemd_installed() -> Result<(), ServiceError> {
    if !systemd_unit_path()?.exists() {
        return Err(ServiceError::NotInstalled);
    }
    Ok(())
}

fn systemd_is_enabled() -> Result<bool, ServiceError> {
    let output = run_output(systemctl_user(["is-enabled", SYSTEMD_SERVICE_NAME]))?;
    Ok(output.status.success() && String::from_utf8_lossy(&output.stdout).trim() == "enabled")
}

fn systemd_is_active() -> Result<bool, ServiceError> {
    let output = run_output(systemctl_user(["is-active", SYSTEMD_SERVICE_NAME]))?;
    Ok(output.status.success() && String::from_utf8_lossy(&output.stdout).trim() == "active")
}

fn systemd_main_pid() -> Result<Option<u32>, ServiceError> {
    let output = run_output(systemctl_user([
        "show",
        SYSTEMD_SERVICE_NAME,
        "--property",
        "MainPID",
        "--value",
    ]))?;
    if !output.status.success() {
        return Ok(None);
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    match stdout.trim().parse::<u32>() {
        Ok(0) | Err(_) => Ok(None),
        Ok(pid) => Ok(Some(pid)),
    }
}

fn systemd_unit_path() -> Result<PathBuf, ServiceError> {
    let home = home_dir()?;
    Ok(home.join(".config/systemd/user").join(SYSTEMD_UNIT_FILE))
}

fn launchd_plist_path() -> Result<PathBuf, ServiceError> {
    let home = home_dir()?;
    Ok(home.join("Library/LaunchAgents").join(LAUNCHD_PLIST_FILE))
}

fn launchd_log_paths() -> Result<(PathBuf, PathBuf), ServiceError> {
    let home = home_dir()?;
    Ok((
        home.join("Library/Logs/ghfs.log"),
        home.join("Library/Logs/ghfs.err.log"),
    ))
}

fn home_dir() -> Result<PathBuf, ServiceError> {
    dirs::home_dir().ok_or(ServiceError::HomeDirNotFound)
}

fn display_home_relative(path: &Path) -> String {
    match dirs::home_dir() {
        Some(home) if path.starts_with(&home) => {
            let rel = path.strip_prefix(&home).unwrap_or(path);
            if rel.as_os_str().is_empty() {
                "~".to_string()
            } else {
                format!("~/{}", rel.display())
            }
        }
        _ => path.display().to_string(),
    }
}

fn is_nix_managed_service(path: &Path) -> bool {
    fs::read_to_string(path)
        .map(|text| text.contains("/nix/store/"))
        .unwrap_or(false)
}

fn print_nix_managed_warning(backend: ServiceBackend) {
    println!(
        "Warning: ghfs service is already managed by {} (appears to be Nix-managed).",
        backend.manager_name()
    );
    println!("Use your NixOS/home-manager configuration to manage the service instead.");
}

fn parse_launchctl_pid(output: &str) -> Option<u32> {
    for line in output.lines() {
        let lowercase = line.to_ascii_lowercase();
        if !lowercase.contains("pid") {
            continue;
        }

        let digits: String = line.chars().filter(|c| c.is_ascii_digit()).collect();
        if let Ok(pid) = digits.parse::<u32>() {
            if pid > 0 {
                return Some(pid);
            }
        }
    }
    None
}

fn run_checked(mut cmd: Command) -> Result<std::process::Output, ServiceError> {
    let command = format!("{cmd:?}");
    let output = cmd.output()?;
    if output.status.success() {
        Ok(output)
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stderr_suffix = if stderr.is_empty() {
            "".to_string()
        } else {
            format!("\n{stderr}")
        };
        Err(ServiceError::CommandFailed {
            command,
            code: output.status.code(),
            stderr: stderr_suffix,
        })
    }
}

fn run_output(mut cmd: Command) -> Result<std::process::Output, ServiceError> {
    Ok(cmd.output()?)
}

fn run_ignore_failure(mut cmd: Command) -> Result<(), ServiceError> {
    let _ = cmd.status()?;
    Ok(())
}

fn run_interactive(mut cmd: Command) -> Result<(), ServiceError> {
    let command = format!("{cmd:?}");
    let status = cmd.status()?;
    if status.success() || status.code() == Some(130) {
        Ok(())
    } else {
        Err(ServiceError::CommandFailed {
            command,
            code: status.code(),
            stderr: "".to_string(),
        })
    }
}

fn systemctl_user<const N: usize>(args: [&str; N]) -> Command {
    let mut cmd = Command::new("systemctl");
    cmd.arg("--user").args(args);
    cmd
}

fn systemctl_journalctl() -> Command {
    let mut cmd = Command::new("journalctl");
    cmd.args(["--user", "-u", SYSTEMD_SERVICE_NAME, "-f"]);
    cmd
}

fn launchctl_cmd(action: &str, path: Option<&Path>, arg: Option<&str>) -> Command {
    let mut cmd = Command::new("launchctl");
    cmd.arg(action);
    if let Some(arg) = arg {
        cmd.arg(arg);
    }
    if let Some(path) = path {
        cmd.arg(path);
    }
    cmd
}

fn launchctl_list(label: &str) -> Result<std::process::Output, ServiceError> {
    let mut cmd = Command::new("launchctl");
    cmd.args(["list", label]);
    run_output(cmd)
}

fn launchd_runtime_state() -> Result<(bool, Option<u32>), ServiceError> {
    let output = launchctl_list(LAUNCHD_LABEL)?;
    if !output.status.success() {
        return Ok((false, None));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let pid = parse_launchctl_pid(&stdout);
    Ok((pid.is_some(), pid))
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn service_path_env() -> String {
    match std::env::var("PATH") {
        Ok(path) if !path.trim().is_empty() => {
            if path.contains("/usr/bin") {
                path
            } else {
                format!("{path}:{DEFAULT_SERVICE_PATH}")
            }
        }
        _ => DEFAULT_SERVICE_PATH.to_string(),
    }
}

fn escape_systemd_value(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn path_has_executable(path_env: &str, name: &str) -> bool {
    std::env::split_paths(path_env)
        .map(|dir| dir.join(name))
        .any(|candidate| candidate.is_file())
}

fn ensure_linux_fuse_helper_available(path_env: &str) -> Result<(), ServiceError> {
    #[cfg(target_os = "linux")]
    {
        if !path_has_executable(path_env, "fusermount3")
            && !path_has_executable(path_env, "fusermount")
        {
            return Err(ServiceError::BackendUnavailable(
                "fusermount helper not found in PATH. Install FUSE userspace tools (usually package 'fuse3').".to_string(),
            ));
        }

        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = path_env;
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn find_open_file_pids(target_path: &str) -> Vec<u32> {
    let mut pids = Vec::new();

    let Ok(proc_dir) = fs::read_dir("/proc") else {
        return pids;
    };

    for entry in proc_dir.flatten() {
        let pid_str = entry.file_name();
        let pid_str = pid_str.to_string_lossy();

        let Ok(pid) = pid_str.parse::<u32>() else {
            continue;
        };

        if pid == std::process::id() {
            continue;
        }

        let fd_dir = format!("/proc/{pid}/fd");
        let Ok(fds) = fs::read_dir(&fd_dir) else {
            continue;
        };

        for fd_entry in fds.flatten() {
            if let Ok(link_target) = fs::read_link(fd_entry.path()) {
                if link_target.to_string_lossy().starts_with(target_path) {
                    pids.push(pid);
                    break;
                }
            }
        }
    }

    pids
}

#[cfg(target_os = "macos")]
fn find_open_file_pids(target_path: &str) -> Vec<u32> {
    let mut cmd = Command::new("lsof");
    cmd.args(["-t", "+D", target_path]);

    let Ok(output) = cmd.output() else {
        return Vec::new();
    };

    if !output.status.success() {
        return Vec::new();
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut pids: Vec<u32> = stdout
        .lines()
        .filter_map(|line| line.trim().parse::<u32>().ok())
        .filter(|pid| *pid != std::process::id())
        .collect();
    pids.sort_unstable();
    pids.dedup();
    pids
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn find_open_file_pids(_target_path: &str) -> Vec<u32> {
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::parse_launchctl_pid;

    #[test]
    fn parse_pid_from_launchctl_output() {
        let sample = "{\n\t\"Label\" = \"com.ghfs.daemon\";\n\t\"PID\" = 67890;\n}";
        assert_eq!(parse_launchctl_pid(sample), Some(67890));
    }

    #[test]
    fn parse_pid_returns_none_when_missing() {
        let sample = "{\n\t\"Label\" = \"com.ghfs.daemon\";\n}";
        assert_eq!(parse_launchctl_pid(sample), None);
    }
}
