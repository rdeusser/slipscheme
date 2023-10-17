package main

import (
	"bytes"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/rdeusser/slipscheme"
)

type Replacements map[string]string

// Set implements flag.Value.
func (r *Replacements) Set(s string) error {
	var ss []string
	n := strings.Count(s, "=")
	switch n {
	case 0:
		return fmt.Errorf("%s must be formatted as key=value", s)
	case 1:
		ss = append(ss, strings.Trim(s, `"`))
	default:
		r := csv.NewReader(strings.NewReader(s))
		var err error
		ss, err = r.Read()
		if err != nil {
			return err
		}
	}
	m := make(map[string]string, len(ss))
	for _, pair := range ss {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) != 2 {
			return fmt.Errorf("%s must be formatted as key=value", pair)
		}
		m[kv[0]] = kv[1]
	}
	*r = m
	return nil
}

// String implements flag.Value.
func (r *Replacements) String() string {
	records := make([]string, 0, len(*r)>>1)
	for k, v := range *r {
		records = append(records, k+"="+v)
	}

	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	if err := w.Write(records); err != nil {
		panic(err)
	}
	w.Flush()
	return "[" + strings.TrimSpace(buf.String()) + "]"
}

func runMain(arguments []string, io slipscheme.Stdio) int {
	flags := flag.NewFlagSet(arguments[0], flag.ExitOnError)
	outputDir := flags.String("dir", ".", "output directory for go files")
	pkgName := flags.String("pkg", "main", "package namespace for go files")
	overwrite := flags.Bool("overwrite", false, "force overwriting existing go files")
	stdout := flags.Bool("stdout", false, "print go code to stdout rather than files")
	format := flags.Bool("fmt", true, "pass code through gofmt")
	comments := flags.Bool("comments", true, "enable/disable print comments")

	replacements := Replacements{}
	flags.Var(&replacements, "replacements", "comma-separated values to replace")

	flags.SetOutput(io.Stderr)
	flags.Parse(arguments[1:])

	processor := slipscheme.NewSchemaProcessor(
		slipscheme.OutputDir(*outputDir),
		slipscheme.PackageName(*pkgName),
		slipscheme.Overwrite(*overwrite),
		slipscheme.Stdout(*stdout),
		slipscheme.Format(*format),
		slipscheme.Comment(*comments),
		slipscheme.IO(io),
		slipscheme.Replacements(replacements),
	)

	args := flags.Args()
	if len(args) == 0 {
		flags.SetOutput(io.Stdout)
		fmt.Fprintf(io.Stdout, "Usage: %s <schema file> [<schema file> ...]\n", arguments[0])
		flags.PrintDefaults()
		return 0
	}

	if err := processor.Process(args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		return 1
	}

	return 0
}

func main() {
	exitCode := runMain(os.Args, slipscheme.Stdio{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	})
	os.Exit(exitCode)
}
