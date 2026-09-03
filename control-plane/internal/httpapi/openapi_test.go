package httpapi

import (
	"os"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/cocoonstack/gateway/control-plane/internal/gateway"
	"github.com/cocoonstack/gateway/control-plane/internal/user"
)

var (
	routePattern  = regexp.MustCompile(`"(GET|POST|PUT|PATCH|DELETE) /api/v1(/[^"]*)"`)
	methodPattern = regexp.MustCompile(`^    (get|post|put|patch|delete):`)
)

func TestRoutesMatchOpenAPISpec(t *testing.T) {
	src, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatalf("read server.go: %v", err)
	}
	served := make(map[string]bool)
	for _, m := range routePattern.FindAllStringSubmatch(string(src), -1) {
		served[strings.ToLower(m[1])+" "+m[2]] = true
	}
	if len(served) == 0 {
		t.Fatal("no routes found in server.go")
	}

	spec := readSpec(t)
	declared := make(map[string]bool)
	inPaths := false
	path := ""
	for line := range strings.SplitSeq(spec, "\n") {
		switch {
		case line == "paths:":
			inPaths = true
		case inPaths && line != "" && !strings.HasPrefix(line, " "):
			inPaths = false
		case inPaths && strings.HasPrefix(line, "  /"):
			path = strings.TrimSuffix(strings.TrimSpace(line), ":")
		case inPaths && methodPattern.MatchString(line):
			method := strings.TrimSuffix(strings.TrimSpace(line), ":")
			declared[method+" "+path] = true
		}
	}
	if len(declared) == 0 {
		t.Fatal("no paths found in openapi.yaml")
	}

	for route := range served {
		if !declared[route] {
			t.Errorf("route %q served but missing from api/openapi.yaml", route)
		}
	}
	for route := range declared {
		if !served[route] {
			t.Errorf("path %q declared in api/openapi.yaml but not served", route)
		}
	}
}

func TestSchemasMirrorGoTypes(t *testing.T) {
	spec := readSpec(t)
	for _, tt := range []struct {
		schema string
		value  any
	}{
		{"User", user.User{}},
		{"UserCreate", userCreate{}},
		{"AccessKey", gateway.Key{}},
	} {
		t.Run(tt.schema, func(t *testing.T) {
			properties, required := schemaFields(t, spec, tt.schema)
			want := jsonFields(reflect.TypeOf(tt.value))
			if !slices.Equal(properties, want) {
				t.Errorf("%s properties = %v, want the Go type's JSON fields %v", tt.schema, properties, want)
			}
			for _, field := range required {
				if !slices.Contains(properties, field) {
					t.Errorf("%s requires %q, which it does not declare", tt.schema, field)
				}
			}
		})
	}
}

func readSpec(t *testing.T) string {
	t.Helper()
	spec, err := os.ReadFile("../../api/openapi.yaml")
	if err != nil {
		t.Fatalf("read openapi.yaml: %v", err)
	}
	return string(spec)
}

func schemaFields(t *testing.T, spec, schema string) (properties, required []string) {
	t.Helper()
	inSchemas, inSchema, inProperties := false, false, false
	for line := range strings.SplitSeq(spec, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		switch indent := len(line) - len(strings.TrimLeft(line, " ")); {
		case indent < 2:
			inSchemas = false
		case indent == 2:
			inSchemas = trimmed == "schemas:"
		case inSchemas && indent == 4:
			inSchema, inProperties = trimmed == schema+":", false
		case inSchema && indent == 6:
			inProperties = trimmed == "properties:"
			if after, ok := strings.CutPrefix(trimmed, "required: ["); ok {
				for field := range strings.SplitSeq(strings.TrimSuffix(after, "]"), ",") {
					required = append(required, strings.TrimSpace(field))
				}
			}
		case inProperties && indent == 8:
			name, _, _ := strings.Cut(trimmed, ":")
			properties = append(properties, name)
		}
	}
	if len(properties) == 0 {
		t.Fatalf("schema %q declares no properties in api/openapi.yaml", schema)
	}
	slices.Sort(properties)
	return properties, required
}

func jsonFields(typ reflect.Type) []string {
	fields := make([]string, 0, typ.NumField())
	for _, field := range reflect.VisibleFields(typ) {
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if name != "" && name != "-" {
			fields = append(fields, name)
		}
	}
	slices.Sort(fields)
	return fields
}
