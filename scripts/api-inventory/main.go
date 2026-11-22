// Command api-inventory compares exported package surfaces using Go export data.
// Run from the repository root; upstream stays isolated in the interop module.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/importer"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
)

type listedPackage struct{ ImportPath, Export string }

func load(dir, path string) (*types.Package, error) {
	cmd := exec.Command("go", "list", "-export", "-deps", "-json", path)
	cmd.Dir = dir
	cmd.Stderr = os.Stderr
	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	exports := map[string]string{}
	dec := json.NewDecoder(strings.NewReader(string(output)))
	for {
		var p listedPackage
		if err := dec.Decode(&p); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		exports[p.ImportPath] = p.Export
	}
	imp := importer.ForCompiler(token.NewFileSet(), "gc", func(path string) (io.ReadCloser, error) { return os.Open(exports[path]) })
	return imp.Import(path)
}

// Gather candidate names, then let LookupFieldOrMethod resolve shadowing and
// ambiguity. Traverse only embedded fields, guarding recursive type graphs.
func collectFields(t types.Type, seen map[types.Type]bool, names map[string]bool) {
	if seen[t] {
		return
	}
	seen[t] = true
	if ptr, ok := t.Underlying().(*types.Pointer); ok {
		collectFields(ptr.Elem(), seen, names)
		return
	}
	st, ok := t.Underlying().(*types.Struct)
	if !ok {
		return
	}
	for i := 0; i < st.NumFields(); i++ {
		f := st.Field(i)
		if f.Exported() {
			names[f.Name()] = true
		}
		if f.Embedded() {
			collectFields(f.Type(), seen, names)
		}
	}
}

func surface(p *types.Package) map[string]string {
	out := map[string]string{}
	qualifier := func(p *types.Package) string {
		path := p.Path()
		for _, base := range []string{"github.com/getlantern/photon", "github.com/quic-go/quic-go"} {
			if path == base {
				return "quic"
			}
			if strings.HasPrefix(path, base+"/") {
				return strings.TrimPrefix(path, base+"/")
			}
		}
		return path
	}
	render := func(t types.Type) string { return types.TypeString(t, qualifier) }
	for _, name := range p.Scope().Names() {
		obj := p.Scope().Lookup(name)
		if !obj.Exported() {
			continue
		}
		switch o := obj.(type) {
		case *types.TypeName:
			typ := o.Type()
			switch u := typ.Underlying().(type) {
			case *types.Struct:
				out["type "+name] = "struct"
				candidates := make(map[string]bool)
				collectFields(typ, make(map[types.Type]bool), candidates)
				for field := range candidates {
					obj, _, _ := types.LookupFieldOrMethod(typ, true, nil, field)
					if f, ok := obj.(*types.Var); ok && f.IsField() {
						out[name+"."+field+" (field)"] = render(f.Type())
					}
				}
			case *types.Interface:
				out["type "+name] = "interface"
				u.Complete()
				for i := 0; i < u.NumMethods(); i++ {
					m := u.Method(i)
					if m.Exported() {
						out[name+"."+m.Name()+" (method)"] = render(m.Type())
					}
				}
			default:
				out["type "+name] = render(typ.Underlying())
			}
			// Pointer method sets include promoted methods and methods of aliases.
			values := types.NewMethodSet(typ)
			for _, target := range []types.Type{typ, types.NewPointer(typ)} {
				methods := types.NewMethodSet(target)
				for i := 0; i < methods.Len(); i++ {
					m := methods.At(i).Obj()
					if m.Exported() {
						kind := " [pointer]"
						if values.Lookup(m.Pkg(), m.Name()) != nil {
							kind = " [value]"
						}
						out[name+"."+m.Name()+" (method)"] = render(m.Type()) + kind
					}
				}
			}
		case *types.Func:
			out["func "+name] = render(o.Type())
		case *types.Const:
			out["const "+name] = render(o.Type()) + " = " + o.Val().ExactString()
		case *types.Var:
			out["var "+name] = render(o.Type())
		}
	}
	return out
}

func main() {
	suffix := flag.String("package", "", "relative package, e.g. http3")
	flag.Parse()
	tail := ""
	if *suffix != "" {
		tail = "/" + *suffix
	}
	own, err := load(".", "github.com/getlantern/photon"+tail)
	if err != nil {
		panic(err)
	}
	peer, err := load("interop", "github.com/quic-go/quic-go"+tail)
	if err != nil {
		panic(err)
	}
	a, b := surface(own), surface(peer)
	keys := make([]string, 0, len(b))
	for k := range b {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Printf("# Exported API comparison: %s\n\n", peer.Path())
	fmt.Println("Generated from Go export data with `go run ./scripts/api-inventory -package=" + *suffix + "`. Reference version is pinned in interop/go.mod. Build tags and platform follow the current Go environment. This inventory includes resolved exported fields and promoted methods, including aliases. It does not prove behavioral compatibility. Declaration differences can include parameter names or internal type aliases and require manual review. Method entries distinguish value from pointer-only availability. Struct tags, interface type sets, generic constraints and constructor ownership require separate review.\n")
	fmt.Println("| Reference symbol | Status | Reference declaration | Local declaration |\n| --- | --- | --- | --- |")
	missing, different, same := 0, 0, 0
	escape := func(s string) string { return strings.ReplaceAll(strings.ReplaceAll(s, "|", "&#124;"), "\n", " ") }
	for _, k := range keys {
		local, ok := a[k]
		status := "same declaration"
		if !ok {
			status = "missing"
			missing++
		} else if local != b[k] {
			status = "review difference"
			different++
		} else {
			same++
		}
		fmt.Printf("| `%s` | %s | `%s` | `%s` |\n", escape(k), status, escape(b[k]), escape(local))
	}
	fmt.Printf("\nReference entries: %d; missing: %d; declaration differences: %d; same declarations: %d.\n", len(b), missing, different, same)
	fmt.Println("Local-only entries are excluded; their presence does not compensate for missing reference capabilities.")
}
