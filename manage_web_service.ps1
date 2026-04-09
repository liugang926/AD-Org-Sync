param(
    [ValidateSet("status", "start", "stop", "restart")]
    [string]$Action = "status",
    [int]$WaitSeconds = 30,
    [string]$PythonExe = "",
    [switch]$Json
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

$ResolvedPython = Resolve-PythonExe -Candidate $PythonExe
$arguments = @("-m", "sync_app.web.windows_service", $Action)
if ($Action -ne "status") {
    $arguments += @("--wait", [string]$WaitSeconds)
}
elseif ($Json.IsPresent) {
    $arguments += "--json"
}

& $ResolvedPython @arguments
exit $LASTEXITCODE
