// Command riquet runs the standalone schema registry.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/k3rnL/riquet/internal/app"
	"github.com/k3rnL/riquet/internal/buildinfo"
	runtimeconfig "github.com/k3rnL/riquet/internal/config"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("riquet", flag.ContinueOnError)
	flags.SetOutput(stderr)
	showVersion := flags.Bool("version", false, "print version information")
	configPath := flags.String("config", "", "YAML configuration file")
	listenAddress := flags.String("listen", "", "public HTTP listen address")
	dataPath := flags.String("data", "", "PVC-backed database path")
	backend := flags.String("backend", "", "storage backend (pvc or kafka)")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *showVersion {
		if _, err := fmt.Fprintln(stdout, buildinfo.Current()); err != nil {
			return 1
		}
		return 0
	}

	var overrides runtimeconfig.Overrides
	flags.Visit(func(item *flag.Flag) {
		switch item.Name {
		case "listen":
			overrides.PublicAddress = listenAddress
		case "data":
			overrides.DataPath = dataPath
		case "backend":
			overrides.Backend = backend
		}
	})
	runtime, err := runtimeconfig.Load(*configPath, nil, overrides)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "riquet: configuration: %v\n", err)
		return 2
	}
	if err := app.Run(ctx, app.Config{Runtime: &runtime}); err != nil {
		_, _ = fmt.Fprintf(stderr, "riquet: %v\n", err)
		return 1
	}
	return 0
}
