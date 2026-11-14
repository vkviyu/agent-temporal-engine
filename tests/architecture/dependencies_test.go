package architecture_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Guard the public package boundary while the implementation grows. Changing
// this table requires an explicit architecture decision, not a hidden import.
func TestProductionDependencyDirection(t *testing.T) {
	const module = "github.com/vkviyu/agent-temporal-engine/"
	allowed := map[string]string{
		".":     "api engine storage graph memory knowledge model transfer",
		"model": "", "codec": "model", "api": "agentprofile annotation model graph investigation memory knowledge transfer",
		// Annotation and investigation are pure public domains over identities.
		// The API, engine and transports consume their types and codecs.
		"annotation":    "model",
		"investigation": "model",
		"state":         "codec model",
		"engine":        "agentprofile annotation api codec eventlog graph investigation knowledge memory model state storage transfer", "reference": "api codec model",
		"tests/reference": "", "formats": "",
		"transfer": "codec memory model",
		"eventlog": "codec model",
		"storage":  "codec model", "journalstore": "model storage", "agentprofile": "codec model journalstore storage", "graphstore": "codec graph model state storage", "knowledge": "codec model journalstore storage", "graph": "codec model", "memory": "codec model journalstore storage", "agentstore": ". agentprofile api model storage",
		"client":   "agentprofile annotation api graph investigation knowledge memory model protocol transfer",
		"protocol": "api model transfer",
		"service":  "agentprofile annotation api graph investigation knowledge memory model protocol transfer",
		"cmd/ate":  ". api model storage service",
	}
	root := filepath.Join("..", "..")
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" {
				return filepath.SkipDir
			}
			if entry.Name() == "internal" {
				t.Errorf("internal directory is forbidden: %s", path)
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		pkg := filepath.ToSlash(rel)
		deps, exists := allowed[pkg]
		if !exists {
			t.Errorf("production package %s has no reviewed dependency policy", pkg)
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imp := range file.Imports {
			name, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				return err
			}
			if name == strings.TrimSuffix(module, "/") || strings.HasPrefix(name, module) {
				dep := "."
				if strings.HasPrefix(name, module) {
					dep = strings.TrimPrefix(name, module)
				}
				if !strings.Contains(" "+deps+" ", " "+dep+" ") {
					t.Errorf("forbidden dependency: %s -> %s", pkg, dep)
				}
			} else if strings.Contains(strings.Split(name, "/")[0], ".") {
				t.Errorf("external production dependency is not allowed: %s -> %s", pkg, name)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
