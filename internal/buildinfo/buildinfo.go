// Package buildinfo exposes immutable build metadata injected by the build.
package buildinfo

import "fmt"

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

// Info describes a Riquet build.
type Info struct {
	Version string
	Commit  string
	Date    string
}

// Current returns metadata for the running binary.
func Current() Info {
	return Info{Version: version, Commit: commit, Date: date}
}

// String renders stable, human-readable build metadata.
func (i Info) String() string {
	return fmt.Sprintf("version=%s commit=%s date=%s", i.Version, i.Commit, i.Date)
}
