package statsdclient

import (
	"strings"
)

// Generates a prefix in the form "environment.app.hostname.", where dots in
// the hostname are replaced with underscores so they don't conflict with stats
// dot namespacing
func MakePrefix(environment, app, hostname string) string {
	underscoreHostname := strings.Replace(hostname, ".", "_", -1)
	return environment + "." + app + "." + underscoreHostname + "."
}
