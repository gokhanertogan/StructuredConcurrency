# Builds the repository with the StructuredConcurrency analyzers added from outside, without touching its project
# files, then reports what they find as annotations, a job summary table and the `findings` output.
#
# The analyzers come from the StructuredConcurrency package on nuget.org. SC0004 (Task.WhenAll that doesn't cancel the
# rest when one operation fails) checks any C# project; SC0001-SC0003 report only in projects that use TaskGroup.

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$version = $env:INPUT_ANALYZER_VERSION
$analyzerPath = $env:INPUT_ANALYZER_PATH
$project = $env:INPUT_PROJECT
$failOnFindings = $env:INPUT_FAIL_ON_FINDINGS -eq 'true'
$buildArguments = $env:INPUT_BUILD_ARGUMENTS
$workspace = if ($env:GITHUB_WORKSPACE) { $env:GITHUB_WORKSPACE } else { (Get-Location).Path }
$work = Join-Path ($env:RUNNER_TEMP ? $env:RUNNER_TEMP : [IO.Path]::GetTempPath()) 'structured-concurrency-check'
$rules = 'SC0001', 'SC0002', 'SC0003', 'SC0004'
$helpUrl = 'https://github.com/gokhanertogan/StructuredConcurrency#analyzers'

# The action lives in the StructuredConcurrency repository, so by default it runs the analyzers of the version its tag
# released: src/Directory.Build.props, next to it, holds that version.
if (-not $version -and -not $analyzerPath) {
    $props = Join-Path $PSScriptRoot '..' 'src' 'Directory.Build.props'
    $version = ([xml](Get-Content -Raw $props)).Project.PropertyGroup.Version | Where-Object { $_ } | Select-Object -First 1
}

if (-not $analyzerPath -and $version -notmatch '^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$') {
    Write-Output "::error title=Task.WhenAll Cancellation Check::analyzer-version '$version' is not a version number."
    exit 1
}
New-Item -ItemType Directory -Force -Path $work | Out-Null

# ---------------------------------------------------------------------------------------------------------------------
# The analyzers, from the package.

$id = 'structuredconcurrency'
$package = Join-Path $work "$id.$version.nupkg"
$analyzer = if ($analyzerPath) { (Resolve-Path $analyzerPath).Path } else { Join-Path $work "StructuredConcurrency.Analyzers.$version.dll" }
if (-not (Test-Path $analyzer)) {
    try {
        Invoke-WebRequest -Uri "https://api.nuget.org/v3-flatcontainer/$id/$version/$id.$version.nupkg" -OutFile $package
    }
    catch {
        # Most likely the action runs from a branch whose version hasn't been released yet.
        Write-Output "::error title=Task.WhenAll Cancellation Check::Couldn't download StructuredConcurrency $version from nuget.org ($($_.Exception.Message)). Use a release tag of the action, such as @v0.11.0, or set analyzer-version."
        exit 1
    }
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $zip = [IO.Compression.ZipFile]::OpenRead($package)
    try {
        $entry = $zip.GetEntry('analyzers/dotnet/cs/StructuredConcurrency.Analyzers.dll')
        if ($null -eq $entry) {
            throw "StructuredConcurrency $version has no analyzers."
        }
        [IO.Compression.ZipFileExtensions]::ExtractToFile($entry, $analyzer, $true)
    }
    finally {
        $zip.Dispose()
    }
}

# ---------------------------------------------------------------------------------------------------------------------
# A targets file that every project imports after its own settings (CustomAfterMicrosoftCommonTargets). It adds the
# analyzers and a .globalconfig that turns SC0004 on without the package and makes it a warning; and it keeps the
# findings from failing a build that treats warnings as errors.

$globalConfig = Join-Path $work 'structured-concurrency-check.globalconfig'
@"
is_global = true
global_level = 100
structured_concurrency.analyze_without_package = true
dotnet_diagnostic.SC0004.severity = warning
"@ | Set-Content -Path $globalConfig -Encoding utf8

$targets = Join-Path $work 'structured-concurrency-check.targets'
$escape = { param($path) [Security.SecurityElement]::Escape($path) }
@"
<Project>
  <PropertyGroup>
    <WarningsNotAsErrors>`$(WarningsNotAsErrors);$($rules -join ';')</WarningsNotAsErrors>
  </PropertyGroup>
  <ItemGroup Condition="'`$(Language)' == 'C#'">
    <Analyzer Include="$(& $escape $analyzer)" />
    <!-- EditorConfigFiles, not GlobalAnalyzerConfigFiles: the SDK has already copied those by the time this runs. -->
    <EditorConfigFiles Include="$(& $escape $globalConfig)" />
  </ItemGroup>
</Project>
"@ | Set-Content -Path $targets -Encoding utf8

# ---------------------------------------------------------------------------------------------------------------------
# The build. --no-incremental, so that every file is compiled and analyzed, even with obj/ restored from a cache.

$arguments = @('build')
if ($project) {
    $arguments += $project
}
$arguments += '--no-incremental', '-nologo', "-p:CustomAfterMicrosoftCommonTargets=$targets"
if ($buildArguments) {
    $arguments += $buildArguments -split '\s+' | Where-Object { $_ }
}

# file(line,col): warning SC0004: message (help link) [project]
$pattern = '^\s*(?<file>.+?)\((?<line>\d+),(?<column>\d+)(?:,\d+,\d+)?\): warning (?<rule>SC\d{4}): (?<message>.*?)(?: \(https?://[^)\s]*\))?(?: \[[^\]]*\])?\s*$'
$findings = [Collections.Generic.List[object]]::new()
$seen = [Collections.Generic.HashSet[string]]::new()

Write-Output "::group::dotnet $($arguments -join ' ')"
& dotnet @arguments 2>&1 | ForEach-Object {
    $line = "$_"
    $match = [regex]::Match($line, $pattern)
    if ($match.Success -and $rules -contains $match.Groups['rule'].Value) {
        # Reported below, once each: a project that targets several frameworks reports every place once per target.
        # Not echoed, so that a problem matcher such as setup-dotnet's doesn't annotate it a second time.
        $file = $match.Groups['file'].Value
        $relative = [IO.Path]::GetRelativePath($workspace, $file).Replace('\', '/')
        $key = "$relative|$($match.Groups['line'].Value)|$($match.Groups['column'].Value)|$($match.Groups['rule'].Value)"
        if ($seen.Add($key)) {
            $findings.Add([pscustomobject]@{
                    File    = $relative
                    Line    = [int]$match.Groups['line'].Value
                    Column  = [int]$match.Groups['column'].Value
                    Rule    = $match.Groups['rule'].Value
                    Message = $match.Groups['message'].Value
                })
        }
    }
    else {
        Write-Output $line
    }
}
$buildExitCode = $LASTEXITCODE
Write-Output '::endgroup::'

if ($buildExitCode -ne 0) {
    Write-Output "::error title=Task.WhenAll Cancellation Check::dotnet build failed (exit code $buildExitCode), so the code wasn't fully checked. The build output above shows why."
    exit $buildExitCode
}

# ---------------------------------------------------------------------------------------------------------------------
# The report.

function Format-Data([string] $value) {
    $value.Replace('%', '%25').Replace("`r", '%0D').Replace("`n", '%0A')
}

function Format-Property([string] $value) {
    (Format-Data $value).Replace(':', '%3A').Replace(',', '%2C')
}

$findings = @($findings | Sort-Object File, Line, Column, Rule)
foreach ($finding in $findings) {
    $properties = "file=$(Format-Property $finding.File),line=$($finding.Line),col=$($finding.Column),title=$(Format-Property $finding.Rule)"
    Write-Output "::warning $properties::$(Format-Data $finding.Message)"
}

if ($env:GITHUB_OUTPUT) {
    "findings=$($findings.Count)" | Add-Content -Path $env:GITHUB_OUTPUT -Encoding utf8
}

if ($env:GITHUB_STEP_SUMMARY) {
    $summary = [Text.StringBuilder]::new()
    [void]$summary.AppendLine('## Task.WhenAll Cancellation Check')
    [void]$summary.AppendLine()
    if ($findings.Count -eq 0) {
        [void]$summary.AppendLine('No findings.')
    }
    else {
        $blob = if ($env:GITHUB_SERVER_URL -and $env:GITHUB_REPOSITORY -and $env:GITHUB_SHA) {
            "$env:GITHUB_SERVER_URL/$env:GITHUB_REPOSITORY/blob/$env:GITHUB_SHA"
        }
        [void]$summary.AppendLine("$($findings.Count) finding(s). See [the rules]($helpUrl) for what each one means and how to fix it.")
        [void]$summary.AppendLine()
        [void]$summary.AppendLine('| Rule | Where | What |')
        [void]$summary.AppendLine('| --- | --- | --- |')
        foreach ($finding in $findings) {
            $where = "$($finding.File):$($finding.Line)"
            if ($blob) {
                $where = "[$where]($blob/$($finding.File)#L$($finding.Line))"
            }
            $what = $finding.Message.Replace('|', '\|')
            [void]$summary.AppendLine("| ``$($finding.Rule)`` | $where | $what |")
        }
    }
    $summary.ToString() | Add-Content -Path $env:GITHUB_STEP_SUMMARY -Encoding utf8
}

Write-Output "$($findings.Count) finding(s)."
if ($failOnFindings -and $findings.Count -gt 0) {
    Write-Output '::error title=Task.WhenAll Cancellation Check::Failing because fail-on-findings is true.'
    exit 1
}
