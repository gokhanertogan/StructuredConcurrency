# Contributing

Thanks for helping. Bug reports, questions about behavior that surprised you, and pull requests are all welcome.

## Before you start

- **Bugs:** open an issue with a small repro, the package version and the .NET version.
- **New features or API changes:** open an issue first. The library is heading for 1.0, and every public member is something it has to keep, so a short discussion before the code saves everyone time.
- **Docs fixes and typos:** send a pull request directly.

## Build and test

You need the .NET 8 and .NET 10 SDKs. The tests run on both, and the benchmarks run on .NET 10.

```sh
git clone https://github.com/gokhanertogan/StructuredConcurrency.git
cd StructuredConcurrency
dotnet test -c Release
```

That builds everything in the solution and runs the library and analyzer tests. CI runs the same command, then packs both packages:

```sh
dotnet pack src/StructuredConcurrency -c Release -o artifacts
dotnet pack src/StructuredConcurrency.Hosting -c Release -o artifacts
```

Warnings are errors, so a build that passes locally with warnings will fail.

## Repository layout

| Path | What it is |
| --- | --- |
| `src/StructuredConcurrency` | The core package: `TaskGroup`, timeouts, hedging, supervisors, channels, `SingleFlight`, `KeyedSequencer` |
| `src/StructuredConcurrency.Hosting` | `StructuredBackgroundService`, a separate package |
| `src/StructuredConcurrency.Analyzers` | The SC0001–SC0004 Roslyn analyzers (netstandard2.0, Roslyn 4.8) |
| `src/StructuredConcurrency.CodeFixes` | Their code fixes. Analyzers and code fixes ship inside the core package |
| `tests/` | xUnit tests for the library and for the analyzers |
| `samples/` | Small console apps that compare the library with the code it replaces |
| `benchmarks/` | BenchmarkDotNet comparisons |
| `action.yml`, `action/` | The GitHub Action that runs the analyzers on other repositories, and the projects its workflows test it on: `action.yml` with the analyzers built from source on every change, `action-release.yml` from each released tag as users run it |
| `src/Directory.Build.props` | Settings shared by both packages, including the version |

## Writing tests

- Tests must be deterministic. Don't use `Task.Delay` to wait for something to happen. Use `TaskCompletionSource` gates to control when work finishes (see `Gated` in `PolicyTests.cs`), and `FakeTimeProvider` for anything that involves time.
- Put a guard timeout on any await that could hang if the code is wrong: `.WaitAsync(Guard)`, where `Guard` is 10 seconds. A broken test then fails instead of hanging CI.
- For cancellation, check that the work actually stopped, not just that the caller got an exception. Not leaving work running is the point of the library.

## Changing the public API

Two checks guard the public API:

- **`PublicAPI.Shipped.txt` and `PublicAPI.Unshipped.txt`** in each package list every public member. If you add, change or remove one, the build fails with RS0016 or RS0017 until the files match. Your IDE's quick fix for RS0016 adds the entry for you, or run `dotnet format analyzers src/StructuredConcurrency --diagnostics RS0016`. New entries go in `PublicAPI.Unshipped.txt`.
- **Package validation** compares the package with the last release and fails `dotnet pack` on a breaking change. Breaking changes need an issue and a decision first.

Every public member needs an XML doc comment. The build fails without one.

## Analyzers

Analyzers live in `src/StructuredConcurrency.Analyzers` and their tests in `tests/StructuredConcurrency.Analyzers.Tests`. A new rule:

- takes the next free ID (`SC0005`, ...),
- is added to `AnalyzerReleases.Unshipped.md`,
- gets tests for both the cases it reports and the cases it must stay quiet on,
- is described in the README's Analyzers section.

The analyzers target netstandard2.0, so newer C# features that need runtime support, such as list patterns, aren't available there.

## Pull requests

- Keep a pull request to one change, and include tests.
- Update the README when behavior or the API changes. It is the main documentation.
- Add a line to the top section of [CHANGELOG.md](CHANGELOG.md) for anything a user of the package would notice. If there is no section for the next version yet, start one with the heading `## Unreleased`.
- Describe what changed and why in the pull request description. If it fixes an issue, link the issue.

Releases are made by the maintainer: rename `## Unreleased` in the changelog to the version, then push a version tag. The release workflow takes the GitHub release notes from that section and stops before publishing if it is missing. You don't need to bump the version.

## License

By contributing, you agree that your contribution is licensed under the [MIT License](LICENSE).
