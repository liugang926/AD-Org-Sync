param(
    [string]$PythonExe = "",
    [string]$DbPath = "",
    [string]$ConfigPath = "",
    [string]$Host = "",
    [int]$Port = 0,
    [string]$PublicBaseUrl = "",
    [string]$SecureCookies = "",
    [switch]$TrustProxyHeaders,
    [string]$ForwardedAllowIps = "",
    [string]$LogPath = "",
    [string]$AdminUsername = "",
    [string]$AdminPassword = "",
    [string]$AdminPasswordEnv = "",
    [string]$Startup = "auto",
    [int]$WaitSeconds = 30
)

$forwardParams = @{}
foreach ($entry in $PSBoundParameters.GetEnumerator()) {
    $forwardParams[$entry.Key] = $entry.Value
}

& (Join-Path $PSScriptRoot "install_web_service.ps1") @forwardParams
exit $LASTEXITCODE
