#!/usr/bin/env pwsh
# Windows equivalent of the Makefile. GNU Make is not installed by default on Windows,
# and the project path contains a space, which Make handles poorly - so this script is
# the supported entry point here. Keep it in sync with the Makefile.
#
# Usage:  .\make.ps1 up
#         .\make.ps1 test

[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [string]$Target = 'help'
)

$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot

function Invoke-Checked {
    param([string]$Exe, [string[]]$Arguments)
    & $Exe @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Exe $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
    }
}

function Compose { param([string[]]$Arguments) Invoke-Checked 'docker' (@('compose') + $Arguments) }
function Uv      { param([string[]]$Arguments) Invoke-Checked 'uv' $Arguments }

function Invoke-Env {
    if (Test-Path .env) {
        Write-Host '.env already exists - leaving it alone.'
        return
    }
    Invoke-Checked 'bash' @('scripts/make-env.sh')
}

switch ($Target.ToLowerInvariant()) {
    'help' {
        Write-Host ''
        Write-Host '  wikiflow targets' -ForegroundColor Cyan
        Write-Host ''
        @(
            @('env',            'Create .env with freshly generated secrets'),
            @('build',          'Build the Airflow image'),
            @('up',             'Start the whole stack'),
            @('down',           'Stop the stack, keep volumes'),
            @('restart',        'Restart the Airflow services only'),
            @('ps',             'Show container status'),
            @('logs',           'Tail logs from all services'),
            @('shell',          'Open a shell inside the scheduler container'),
            @('test',           'Run unit tests locally (fast, no containers)'),
            @('test-container', 'Run DAG + integration tests inside the container'),
            @('lint',           'Check formatting and lint rules'),
            @('format',         'Auto-fix formatting and lint rules'),
            @('typecheck',      'Type-check src/ and tests/'),
            @('check',          'Everything CI runs, locally'),
            @('reset',          'Destroy the stack INCLUDING volumes'),
            @('pin-images',     'Resolve floating image tags to digests')
        ) | ForEach-Object { '    {0,-16} {1}' -f $_[0], $_[1] }
        Write-Host ''
    }
    'env'     { Invoke-Env }
    'build'   { Invoke-Env; Compose @('build') }
    'up'      {
        Invoke-Env
        Compose @('up', '-d')
        Write-Host ''
        Write-Host '  Airflow UI  ->  http://localhost:8080   (airflow / airflow)' -ForegroundColor Green
        Write-Host '  MinIO       ->  http://localhost:9001' -ForegroundColor Green
        Write-Host ''
    }
    'down'    { Compose @('down') }
    'restart' { Compose @('restart', 'airflow-scheduler', 'airflow-apiserver', 'airflow-dag-processor', 'airflow-triggerer') }
    'ps'      { Compose @('ps') }
    'logs'    { Compose @('logs', '-f', '--tail=100') }
    'shell'   { Compose @('exec', 'airflow-scheduler', 'bash') }

    'test'           { Uv @('run', 'pytest', 'tests/unit', '-m', 'unit') }
    'test-container' { Compose @('run', '--rm', '--no-deps', 'airflow-scheduler', 'pytest', '/opt/airflow/tests', '-m', 'dag or integration') }

    'lint'      { Uv @('run', 'ruff', 'check', '.'); Uv @('run', 'ruff', 'format', '--check', '.') }
    'format'    { Uv @('run', 'ruff', 'check', '--fix', '.'); Uv @('run', 'ruff', 'format', '.') }
    'typecheck' { Uv @('run', 'mypy') }
    'check'     { & $PSCommandPath lint; & $PSCommandPath typecheck; & $PSCommandPath test }

    'reset'      { Compose @('down', '-v') }
    'pin-images' { Invoke-Checked 'bash' @('scripts/pin-images.sh') }

    default {
        Write-Host "Unknown target: $Target" -ForegroundColor Red
        & $PSCommandPath help
        exit 1
    }
}
