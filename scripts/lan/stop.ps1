# Stops instances started by these scripts (never your own manually started servers).
# Usage: stop.ps1            -> stop all
#        stop.ps1 -Name friend
param([ValidateSet('all', 'me', 'friend')][string]$Name = 'all')
. (Join-Path $PSScriptRoot 'lan.ps1')
$names = if ($Name -eq 'all') { @('friend', 'me') } else { @($Name) }
foreach ($n in $names) { Stop-Instance $n }
