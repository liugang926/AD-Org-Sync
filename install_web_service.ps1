param(
    [string]$PythonExe = "",
    [string]$DbPath = ".appdata\\local_web.db",
    [string]$ConfigPath = "config.ini",
    [string]$Host = "127.0.0.1",
    [int]$Port = 8010,
    [string]$PublicBaseUrl = "",
    [ValidateSet("auto", "always", "never")]
    [string]$SecureCookies = "auto",
    [switch]$TrustProxyHeaders,
    [string]$ForwardedAllowIps = "",
    [string]$LogPath = "",
    [string]$AdminUsername = "",
    [string]$AdminPassword = "",
    [string]$AdminPasswordEnv = "",
    [ValidateSet("auto", "delayed", "manual", "disabled")]
    [string]$Startup = "auto",
    [int]$WaitSeconds = 30
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

function Resolve-PythonExe {
    param([string]$Candidate)

    if ($Candidate) {
        return [System.IO.Path]::GetFullPath($Candidate)
    }
    foreach ($path in @(".venv\\Scripts\\python.exe", "venv\\Scripts\\python.exe")) {
        $fullPath = Join-Path $Root $path
        if (Test-Path $fullPath) {
            return $fullPath
        }
    }
    throw "Python executable not found. Run install_web_env.bat first or pass -PythonExe."
}

function Resolve-AbsolutePath {
    param([string]$Value)

    if (-not $Value) {
        return ""
    }
    if ([System.IO.Path]::IsPathRooted($Value)) {
        return [System.IO.Path]::GetFullPath($Value)
    }
    return [System.IO.Path]::GetFullPath((Join-Path $Root $Value))
}

function Invoke-Python {
    param([string[]]$Arguments)
    & $script:ResolvedPython @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed: $($Arguments -join ' ')"
    }
}

function Invoke-WebProbe {
    param([string]$Url)

    try {
        $response = Invoke-WebRequest -Uri $Url -TimeoutSec 5 -MaximumRedirection 0 -UseBasicParsing
        return @{
            StatusCode = [int]$response.StatusCode
            Body = [string]$response.Content
        }
    }
    catch {
        if ($_.Exception.Response) {
            $statusCode = [int]$_.Exception.Response.StatusCode.value__
            $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
            $body = $reader.ReadToEnd()
            $reader.Dispose()
            return @{
                StatusCode = $statusCode
                Body = $body
            }
        }
        throw
    }
}

$ResolvedPython = Resolve-PythonExe -Candidate $PythonExe
$ResolvedDbPath = Resolve-AbsolutePath -Value $DbPath
$ResolvedConfigPath = Resolve-AbsolutePath -Value $ConfigPath
$ResolvedLogPath = if ($LogPath) { Resolve-AbsolutePath -Value $LogPath } else { "" }

Write-Host "[INFO] Installing deployment dependencies into the virtual environment..."
Invoke-Python -Arguments @("-m", "pip", "install", "--upgrade", "pip")
Invoke-Python -Arguments @("-m", "pip", "install", "-r", "requirements-deploy.txt")

Write-Host "[INFO] Initializing local database and default organization..."
Invoke-Python -Arguments @("-m", "sync_app.cli", "init-web", "--db-path", $ResolvedDbPath, "--config", $ResolvedConfigPath)

if ($AdminUsername) {
    if (-not $AdminPassword -and -not $AdminPasswordEnv) {
        throw "When -AdminUsername is provided you must also pass -AdminPassword or -AdminPasswordEnv."
    }
    $bootstrapArgs = @(
        "-m", "sync_app.cli", "bootstrap-admin",
        "--db-path", $ResolvedDbPath,
        "--username", $AdminUsername,
        "--reset",
        "--enable"
    )
    if ($AdminPassword) {
        $bootstrapArgs += @("--password", $AdminPassword)
    }
    if ($AdminPasswordEnv) {
        $bootstrapArgs += @("--password-env", $AdminPasswordEnv)
    }
    Write-Host "[INFO] Bootstrapping administrator account..."
    Invoke-Python -Arguments $bootstrapArgs
}

$statusJson = & $ResolvedPython -m sync_app.web.windows_service status --json
if ($LASTEXITCODE -ne 0) {
    throw "Unable to query Windows service state."
}
$serviceStatus = $statusJson | ConvertFrom-Json
$serviceCommand = if ($serviceStatus.installed) { "update" } else { "install" }

$serviceArgs = @(
    "-m", "sync_app.web.windows_service", $serviceCommand,
    "--db-path", $ResolvedDbPath,
    "--config", $ResolvedConfigPath,
    "--host", $Host,
    "--port", [string]$Port,
    "--secure-cookies", $SecureCookies,
    "--startup", $Startup
)
if ($PublicBaseUrl) {
    $serviceArgs += @("--public-base-url", $PublicBaseUrl)
}
if ($TrustProxyHeaders.IsPresent) {
    $serviceArgs += "--trust-proxy-headers"
}
if ($ForwardedAllowIps) {
    $serviceArgs += @("--forwarded-allow-ips", $ForwardedAllowIps)
}
if ($ResolvedLogPath) {
    $serviceArgs += @("--log-path", $ResolvedLogPath)
}

Write-Host "[INFO] Applying Windows service configuration..."
Invoke-Python -Arguments $serviceArgs

if ($serviceStatus.installed -and $serviceStatus.state -eq "running") {
    Write-Host "[INFO] Restarting Windows service..."
    Invoke-Python -Arguments @("-m", "sync_app.web.windows_service", "restart", "--wait", [string]$WaitSeconds)
}
else {
    Write-Host "[INFO] Starting Windows service..."
    Invoke-Python -Arguments @("-m", "sync_app.web.windows_service", "start", "--wait", [string]$WaitSeconds)
}

$healthUrl = "http://{0}:{1}/healthz" -f $Host, $Port
$readyUrl = "http://{0}:{1}/readyz" -f $Host, $Port
$health = Invoke-WebProbe -Url $healthUrl
if ($health.StatusCode -ne 200) {
    throw "Health probe failed: $healthUrl returned $($health.StatusCode)"
}

$ready = Invoke-WebProbe -Url $readyUrl
Write-Host "[OK] Service is reachable: $healthUrl"
if ($ready.StatusCode -eq 200) {
    Write-Host "[OK] Service is ready: $readyUrl"
}
else {
    Write-Warning "Service is running but not fully ready yet ($readyUrl -> $($ready.StatusCode)). Complete /setup or fix readiness checks."
}

Write-Host "[INFO] Windows service name: ADOrgSyncWeb"
Write-Host "[INFO] Management command: .\\manage_web_service.ps1 -Action status"
