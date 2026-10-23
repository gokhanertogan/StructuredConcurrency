## Release 0.8.0

### New Rules

Rule ID | Category | Severity | Notes
--------|----------|----------|-------
SC0003 | Reliability | Warning | Work started with Task.Run and the like escapes the task group

## Release 0.6.0

### New Rules

Rule ID | Category | Severity | Notes
--------|----------|----------|-------
SC0001 | Reliability | Warning | The cancellation token passed to the work is never used
SC0002 | Reliability | Warning | The work uses an outer cancellation token instead of its own
