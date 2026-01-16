package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

type goListPackage struct {
	ImportPath string
	Name       string
	Dir        string
	GoFiles    []string
	CgoFiles   []string
}

type pkgAPI struct {
	ImportPath string
	Name       string

	Types   []string
	Funcs   []string
	Consts  []string
	Vars    []string
	Methods []string
}

type checklistKey struct {
	Package string
	Section string
	Item    string
}

type checklistEntry struct {
	Checked bool
	Rust    string
	Tests   string
}

func main() {
	var clientGoDir string
	var outInventory string
	var outChecklist string

	flag.StringVar(&clientGoDir, "client-go-dir", "client-go", "path to the client-go checkout")
	flag.StringVar(
		&outInventory,
		"out-inventory",
		".codex/progress/client-go-api-inventory.md",
		"output markdown file for the signature-level inventory",
	)
	flag.StringVar(
		&outChecklist,
		"out-checklist",
		".codex/progress/parity-checklist.md",
		"output markdown file for the parity checklist skeleton",
	)
	flag.Parse()

	modulePath, err := readGoModulePath(filepath.Join(clientGoDir, "go.mod"))
	if err != nil {
		fatalf("read go.mod module path: %v", err)
	}

	pkgs, err := listGoPackages(clientGoDir)
	if err != nil {
		fatalf("go list: %v", err)
	}

	apis := make([]pkgAPI, 0, len(pkgs))
	for _, pkg := range pkgs {
		rel := importPathRel(modulePath, pkg.ImportPath)
		if shouldSkipPackage(rel) {
			continue
		}
		api, err := extractPackageAPI(pkg)
		if err != nil {
			fatalf("extract %s: %v", pkg.ImportPath, err)
		}
		apis = append(apis, api)
	}
	sort.Slice(apis, func(i, j int) bool { return apis[i].ImportPath < apis[j].ImportPath })

	inv := renderInventory(modulePath, apis)
	if err := writeFileAtomic(outInventory, inv); err != nil {
		fatalf("write inventory: %v", err)
	}

	existingChecklist, err := readChecklist(outChecklist)
	if err != nil {
		fatalf("read existing checklist: %v", err)
	}
	chk := renderChecklist(modulePath, apis, existingChecklist)
	if err := writeFileAtomic(outChecklist, chk); err != nil {
		fatalf("write checklist: %v", err)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func readGoModulePath(goModPath string) (string, error) {
	b, err := os.ReadFile(goModPath)
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "//") {
			continue
		}
		if after, ok := strings.CutPrefix(line, "module "); ok {
			return strings.TrimSpace(after), nil
		}
	}
	return "", fmt.Errorf("module directive not found in %s", goModPath)
}

func listGoPackages(clientGoDir string) ([]goListPackage, error) {
	cmd := exec.Command("go", "list", "-json", "./...")
	cmd.Dir = clientGoDir
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	defer cmd.Wait()

	dec := json.NewDecoder(stdout)
	var pkgs []goListPackage
	for {
		var p goListPackage
		if err := dec.Decode(&p); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		pkgs = append(pkgs, p)
	}
	return pkgs, cmd.Wait()
}

func importPathRel(modulePath, importPath string) string {
	if importPath == modulePath {
		return ""
	}
	rel := strings.TrimPrefix(importPath, modulePath)
	rel = strings.TrimPrefix(rel, "/")
	return rel
}

func shouldSkipPackage(relImportPath string) bool {
	switch {
	case relImportPath == "":
		return true
	case strings.HasPrefix(relImportPath, "internal/"):
		return true
	case strings.HasPrefix(relImportPath, "integration_tests/"):
		return true
	case strings.HasPrefix(relImportPath, "examples/"):
		return true
	case relImportPath == "testutils":
		return true
	case strings.HasPrefix(relImportPath, "testutils/"):
		return true
	default:
		return false
	}
}

func extractPackageAPI(pkg goListPackage) (pkgAPI, error) {
	api := pkgAPI{
		ImportPath: pkg.ImportPath,
		Name:       pkg.Name,
	}

	fset := token.NewFileSet()
	files := append(append([]string(nil), pkg.GoFiles...), pkg.CgoFiles...)
	sort.Strings(files)
	for _, file := range files {
		path := filepath.Join(pkg.Dir, file)
		parsed, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return pkgAPI{}, err
		}

		for _, decl := range parsed.Decls {
			switch decl := decl.(type) {
			case *ast.GenDecl:
				switch decl.Tok {
				case token.TYPE:
					for _, spec := range decl.Specs {
						spec, ok := spec.(*ast.TypeSpec)
						if !ok || !ast.IsExported(spec.Name.Name) {
							continue
						}
						api.Types = append(api.Types, formatTypeSpec(fset, spec))
					}
				case token.CONST:
					for _, name := range exportedValueSpecNames(decl.Specs) {
						api.Consts = append(api.Consts, name)
					}
				case token.VAR:
					for _, name := range exportedValueSpecNames(decl.Specs) {
						api.Vars = append(api.Vars, name)
					}
				}
			case *ast.FuncDecl:
				if !ast.IsExported(decl.Name.Name) {
					continue
				}
				if decl.Recv == nil {
					api.Funcs = append(api.Funcs, formatFuncDeclSignature(fset, decl))
					continue
				}
				if recvBase := receiverBaseIdent(decl.Recv); recvBase == "" || !ast.IsExported(recvBase) {
					continue
				}
				api.Methods = append(api.Methods, formatFuncDeclSignature(fset, decl))
			}
		}
	}

	sort.Strings(api.Types)
	sort.Strings(api.Funcs)
	sort.Strings(api.Consts)
	sort.Strings(api.Vars)
	sort.Strings(api.Methods)

	return api, nil
}

func exportedValueSpecNames(specs []ast.Spec) []string {
	var names []string
	for _, spec := range specs {
		vs, ok := spec.(*ast.ValueSpec)
		if !ok {
			continue
		}
		for _, ident := range vs.Names {
			if ident != nil && ast.IsExported(ident.Name) {
				names = append(names, ident.Name)
			}
		}
	}
	return names
}

func formatTypeSpec(fset *token.FileSet, spec *ast.TypeSpec) string {
	name := spec.Name.Name

	if spec.Assign.IsValid() {
		return fmt.Sprintf("type %s = %s", name, formatExpr(fset, spec.Type))
	}

	switch spec.Type.(type) {
	case *ast.StructType:
		return fmt.Sprintf("type %s struct", name)
	case *ast.InterfaceType:
		return fmt.Sprintf("type %s interface", name)
	default:
		return fmt.Sprintf("type %s %s", name, formatExpr(fset, spec.Type))
	}
}

func formatFuncDeclSignature(fset *token.FileSet, decl *ast.FuncDecl) string {
	c := *decl
	c.Body = nil
	c.Doc = nil

	var buf bytes.Buffer
	cfg := &printer.Config{Mode: printer.UseSpaces | printer.TabIndent, Tabwidth: 8}
	cfg.Fprint(&buf, fset, &c)
	return collapseWS(buf.String())
}

func formatExpr(fset *token.FileSet, e ast.Expr) string {
	var buf bytes.Buffer
	cfg := &printer.Config{Mode: printer.UseSpaces | printer.TabIndent, Tabwidth: 8}
	cfg.Fprint(&buf, fset, e)
	return collapseWS(buf.String())
}

func collapseWS(s string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(s)), " ")
}

func receiverBaseIdent(recv *ast.FieldList) string {
	if recv == nil || len(recv.List) == 0 || recv.List[0] == nil {
		return ""
	}
	return baseIdent(recv.List[0].Type)
}

func baseIdent(expr ast.Expr) string {
	switch expr := expr.(type) {
	case *ast.Ident:
		return expr.Name
	case *ast.StarExpr:
		return baseIdent(expr.X)
	case *ast.ParenExpr:
		return baseIdent(expr.X)
	case *ast.IndexExpr:
		return baseIdent(expr.X)
	case *ast.IndexListExpr:
		return baseIdent(expr.X)
	case *ast.SelectorExpr:
		return expr.Sel.Name
	default:
		return ""
	}
}

func renderInventory(modulePath string, apis []pkgAPI) []byte {
	var b strings.Builder
	fmt.Fprintln(&b, "# client-go v2 API inventory (signature-level, auto-generated)")
	fmt.Fprintln(&b)
	fmt.Fprintf(&b, "Module: `%s`\n\n", modulePath)
	fmt.Fprintln(&b, "Generated from local source under `client-go/` (excluding `internal/`, `integration_tests/`, `examples/`, `testutils/`).")
	fmt.Fprintln(&b, "This is a signature-level inventory used to drive parity work in this repo's Rust client crate (repo root).")
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "Regenerate:")
	fmt.Fprintln(&b, "- `go run ./tools/client-go-api-inventory`")
	fmt.Fprintln(&b)

	for _, api := range apis {
		rel := importPathRel(modulePath, api.ImportPath)
		fmt.Fprintf(&b, "## %s (package %s)\n\n", rel, api.Name)
		renderSection(&b, "Types", api.Types, func(s string) string { return "`" + s + "`" })
		renderSection(&b, "Functions", api.Funcs, func(s string) string { return "`" + s + "`" })
		renderSection(&b, "Consts", api.Consts, func(s string) string { return "`" + s + "`" })
		renderSection(&b, "Vars", api.Vars, func(s string) string { return "`" + s + "`" })
		renderSection(&b, "Methods", api.Methods, func(s string) string { return "`" + s + "`" })
	}

	return []byte(b.String())
}

func renderChecklist(modulePath string, apis []pkgAPI, existing map[checklistKey]checklistEntry) []byte {
	var b strings.Builder
	fmt.Fprintln(&b, "# client-go v2 parity checklist (auto-generated skeleton)")
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "This file is generated from `client-go/` source and is used to track Go→Rust parity work.")
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "Regenerate:")
	fmt.Fprintln(&b, "- `go run ./tools/client-go-api-inventory`")
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "Conventions:")
	fmt.Fprintln(&b, "- `Rust:` fill with target Rust path/symbol(s)")
	fmt.Fprintln(&b, "- `Tests:` fill with relevant unit/integration tests (or `N/A`)")
	fmt.Fprintln(&b)

	for _, api := range apis {
		rel := importPathRel(modulePath, api.ImportPath)
		fmt.Fprintf(&b, "## %s (package %s)\n\n", rel, api.Name)
		renderChecklistSection(&b, rel, "Types", api.Types, existing)
		renderChecklistSection(&b, rel, "Functions", api.Funcs, existing)
		renderChecklistSection(&b, rel, "Consts", api.Consts, existing)
		renderChecklistSection(&b, rel, "Vars", api.Vars, existing)
		renderChecklistSection(&b, rel, "Methods", api.Methods, existing)
	}

	return []byte(b.String())
}

func renderSection(b *strings.Builder, title string, items []string, format func(string) string) {
	fmt.Fprintf(b, "### %s\n", title)
	if len(items) == 0 {
		fmt.Fprintln(b, "- (none)")
		fmt.Fprintln(b)
		return
	}
	for _, it := range items {
		fmt.Fprintf(b, "- %s\n", format(it))
	}
	fmt.Fprintln(b)
}

func renderChecklistSection(
	b *strings.Builder,
	pkgRel string,
	title string,
	items []string,
	existing map[checklistKey]checklistEntry,
) {
	fmt.Fprintf(b, "### %s\n", title)
	if len(items) == 0 {
		fmt.Fprintln(b, "- (none)")
		fmt.Fprintln(b)
		return
	}
	for _, it := range items {
		key := checklistKey{
			Package: pkgRel,
			Section: title,
			Item:    it,
		}
		entry, ok := existing[key]
		checked := " "
		rust := ""
		tests := ""
		if ok {
			if entry.Checked {
				checked = "x"
			}
			rust = entry.Rust
			tests = entry.Tests
		}
		fmt.Fprintf(b, "- [%s] `%s` | Rust: %s | Tests: %s\n", checked, it, rust, tests)
	}
	fmt.Fprintln(b)
}

func readChecklist(path string) (map[checklistKey]checklistEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[checklistKey]checklistEntry{}, nil
		}
		return nil, err
	}
	defer f.Close()

	entries := make(map[checklistKey]checklistEntry)
	var currentPkg string
	var currentSection string

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		switch {
		case strings.HasPrefix(line, "## "):
			rest := strings.TrimSpace(strings.TrimPrefix(line, "## "))
			pkg, _, ok := strings.Cut(rest, " (package ")
			if ok {
				currentPkg = strings.TrimSpace(pkg)
			} else {
				currentPkg = rest
			}
			currentSection = ""
		case strings.HasPrefix(line, "### "):
			currentSection = strings.TrimSpace(strings.TrimPrefix(line, "### "))
		case strings.HasPrefix(line, "- ["):
			if currentPkg == "" || currentSection == "" {
				continue
			}
			item, entry, ok := parseChecklistItem(line)
			if !ok {
				continue
			}
			entries[checklistKey{
				Package: currentPkg,
				Section: currentSection,
				Item:    item,
			}] = entry
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}

func parseChecklistItem(line string) (string, checklistEntry, bool) {
	var entry checklistEntry
	if len(line) < len("- [ ] `") || !strings.HasPrefix(line, "- [") || line[4] != ']' {
		return "", entry, false
	}
	switch line[3] {
	case 'x', 'X':
		entry.Checked = true
	case ' ':
	default:
		return "", entry, false
	}

	start := strings.IndexByte(line, '`')
	if start == -1 {
		return "", entry, false
	}
	end := strings.IndexByte(line[start+1:], '`')
	if end == -1 {
		return "", entry, false
	}
	end = start + 1 + end
	item := line[start+1 : end]

	rest := strings.TrimSpace(line[end+1:])
	if rest == "" {
		return item, entry, true
	}
	for _, part := range strings.Split(rest, "|") {
		part = strings.TrimSpace(part)
		switch {
		case strings.HasPrefix(part, "Rust:"):
			entry.Rust = strings.TrimSpace(strings.TrimPrefix(part, "Rust:"))
		case strings.HasPrefix(part, "Tests:"):
			entry.Tests = strings.TrimSpace(strings.TrimPrefix(part, "Tests:"))
		}
	}

	return item, entry, true
}

func writeFileAtomic(path string, content []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(content); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
