package formats_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
)

// Inventory every production service source, including platform-specific files.
// Routing must use static Method switches or equality checks. Keep this parser
// in sync if that convention changes; it is not a general control-flow proof.
func protocolDispatchMethods(source fs.FS) (map[string]bool, error) {
	entries, err := fs.ReadDir(source, ".")
	if err != nil {
		return nil, err
	}
	methods := make(map[string]bool)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := fs.ReadFile(source, name)
		if err != nil {
			return nil, err
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, raw, 0)
		if err != nil {
			return nil, err
		}
		var routeErr error
		add := func(expr ast.Expr) {
			literal, ok := expr.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				routeErr = fmt.Errorf("%s: wire method route is not a static string", name)
				return
			}
			value, err := strconv.Unquote(literal.Value)
			if err != nil {
				routeErr = err
				return
			}
			methods[value] = true
		}
		ast.Inspect(file, func(node ast.Node) bool {
			if routeErr != nil {
				return false
			}
			switch node := node.(type) {
			case *ast.SwitchStmt:
				if methodSelector(node.Tag) {
					for _, clause := range node.Body.List {
						for _, expr := range clause.(*ast.CaseClause).List {
							add(expr)
						}
					}
				}
			case *ast.BinaryExpr:
				if node.Op == token.EQL || node.Op == token.NEQ {
					if methodSelector(node.X) {
						add(node.Y)
					} else if methodSelector(node.Y) {
						add(node.X)
					}
				}
			}
			return true
		})
		if routeErr != nil {
			return nil, routeErr
		}
	}
	if len(methods) == 0 {
		return nil, fmt.Errorf("no protocol method routes found")
	}
	return methods, nil
}

func methodSelector(expr ast.Expr) bool {
	selector, ok := expr.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == "Method"
}

func compareProtocolMethods(registered, actual map[string]bool) error {
	var mismatches []string
	for method := range actual {
		if !registered[method] {
			mismatches = append(mismatches, "production wire method absent from registry: "+method)
		}
	}
	for method := range registered {
		if !actual[method] {
			mismatches = append(mismatches, "registry advertises absent wire method: "+method)
		}
	}
	if len(mismatches) == 0 {
		return nil
	}
	slices.Sort(mismatches)
	return fmt.Errorf("%s", strings.Join(mismatches, "\n"))
}

func TestProtocolInventoryDetectsMovedAndUndeclaredRoutes(t *testing.T) {
	source := fstest.MapFS{
		"session.go": {Data: []byte(`package service
func hello(request Request) { if request.Method == "hello" {} }`)},
		"new_dispatch.go": {Data: []byte(`package service
func moved(message Request) { switch message.Method { case "read", "write": } }`)},
		"fake_test.go": {Data: []byte(`package service
func fixture(req Request) { switch req.Method { case "not-production": } }`)},
	}
	registered := map[string]bool{"hello": true, "read": true, "write": true}
	check := func(wantMismatch bool) {
		t.Helper()
		actual, err := protocolDispatchMethods(source)
		if err != nil {
			t.Fatal(err)
		}
		err = compareProtocolMethods(registered, actual)
		if (err != nil) != wantMismatch {
			t.Fatalf("inventory mismatch=%v want=%v; methods=%v", err, wantMismatch, actual)
		}
	}
	check(false)
	source["moved_again.go"] = source["new_dispatch.go"]
	delete(source, "new_dispatch.go")
	check(false)
	source["additional.go"] = &fstest.MapFile{Data: []byte(`package service
func added(incoming Request) { if "undeclared" == incoming.Method {} }`)}
	check(true)
	delete(source, "additional.go")
	delete(source, "moved_again.go")
	check(true)
	source["dynamic.go"] = &fstest.MapFile{Data: []byte(`package service
func dynamic(request Request) { switch request.Method { case methodVariable: } }`)}
	if _, err := protocolDispatchMethods(source); err == nil {
		t.Fatal("dynamic method route silently skipped")
	}
}
