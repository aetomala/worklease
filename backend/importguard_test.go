package backend_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const rootImportPath = "github.com/aetomala/worklease"

// backendDirs lists package backend and the backend implementation packages,
// relative to this package's directory.
var backendDirs = []string{".", "memory", "postgres", "conformance"}

// forEachImport calls fn with the slash-separated file path, the import spec,
// and the unquoted import path for every import of every non-test Go file in
// backendDirs. It fails the test if a directory cannot be read, a file cannot
// be parsed, or a directory has no non-test Go file.
func forEachImport(t *testing.T, fn func(path string, imp *ast.ImportSpec, importPath string)) {
	t.Helper()
	for _, dir := range backendDirs {
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
				fn(filepath.ToSlash(path), imp, p)
			}
		}
		if checked == 0 {
			t.Errorf("%s: no non-test Go files to check", dir)
		}
	}
}

// TestBackendPackagesDoNotImportRoot fails if a non-test Go file of package
// backend or of a backend implementation package imports the root package.
func TestBackendPackagesDoNotImportRoot(t *testing.T) {
	forEachImport(t, func(path string, _ *ast.ImportSpec, importPath string) {
		if importPath == rootImportPath {
			t.Errorf("%s imports %s", path, rootImportPath)
		}
	})
}

// TestBackendPackagesImportNoDriver fails if a non-test Go file of package
// backend or of a backend implementation package has a blank import, such as
// a database driver registered for its side effect.
func TestBackendPackagesImportNoDriver(t *testing.T) {
	forEachImport(t, func(path string, imp *ast.ImportSpec, importPath string) {
		if imp.Name != nil && imp.Name.Name == "_" {
			t.Errorf("%s has a blank import of %s", path, importPath)
		}
	})
}
