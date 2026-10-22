package backend_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const rootImportPath = "github.com/aetomala/worklease"

// TestBackendPackagesDoNotImportRoot fails if a non-test Go file of package
// backend or of a backend implementation package imports the root package.
func TestBackendPackagesDoNotImportRoot(t *testing.T) {
	dirs := []string{".", "memory", "postgres", "conformance"}
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read %s: %v", dir, err)
		}
		checked := 0
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			checked++
			path := filepath.Join(dir, name)
			f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			for _, imp := range f.Imports {
				p, err := strconv.Unquote(imp.Path.Value)
				if err != nil {
					t.Fatalf("unquote %s in %s: %v", imp.Path.Value, path, err)
				}
				if p == rootImportPath {
					t.Errorf("%s imports %s", filepath.ToSlash(path), rootImportPath)
				}
			}
		}
		if checked == 0 {
			t.Errorf("%s: no non-test Go files to check", dir)
		}
	}
}
