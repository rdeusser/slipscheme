package slipscheme

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"unicode"

	"github.com/iancoleman/strcase"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

var (
	caser               = cases.Title(language.Und)
	defaultReplacements = map[string]string{
		"Id":    "ID",
		"Http":  "HTTP",
		"Https": "HTTPS",
		"Api":   "API",
		"Url":   "URL",
		"Json":  "JSON",
		"Xml":   "XML",
		"Html":  "HTML",
	}
)

// Stdio holds common io readers/writers
type Stdio struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// Schema represents JSON schema.
type Schema struct {
	Title                string             `json:"title,omitempty"`
	ID                   string             `json:"id,omitempty"`
	Type                 SchemaType         `json:"type,omitempty"`
	Description          string             `json:"description,omitempty"`
	Definitions          map[string]*Schema `json:"definitions,omitempty"`
	Properties           map[string]*Schema `json:"properties,omitempty"`
	AdditionalProperties bool               `json:"additionalProperties,omitempty"`
	PatternProperties    map[string]*Schema `json:"patternProperties,omitempty"`
	Ref                  string             `json:"$ref,omitempty"`
	Items                *Schema            `json:"items,omitempty"`
	OneOf                []*Schema          `json:"oneOf,omitempty"`
	Const                string             `json:"const,omitempty"`
	Enum                 []string           `json:"enum,omitempty"`
	Root                 *Schema            `json:"-"`
	// only populated on Root node
	raw map[string]any
}

// Name will attempt to determine the name of the Schema element using
// the Title or ID or Description (in that order)
func (s *Schema) Name() string {
	name := s.Title
	if name == "" {
		parts := strings.Split(s.ID, string(filepath.Separator))
		name = strings.Map(func(r rune) rune {
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				return r
			}
			return -1
		}, parts[len(parts)-1])
	}
	if name == "" {
		return s.Description
	}
	return name
}

// SchemaType is an ENUM that is set on parsing the schema
type SchemaType int

const (
	// ANY is a schema element that has no defined type
	ANY SchemaType = iota
	// ARRAY is a schema type "array"
	ARRAY SchemaType = iota
	// BOOLEAN is a schema type "boolean"
	BOOLEAN SchemaType = iota
	// INTEGER is a schema type "integer"
	INTEGER SchemaType = iota
	// NUMBER is a schema type "number"
	NUMBER SchemaType = iota
	// NULL is a schema type "null"
	NULL SchemaType = iota
	// OBJECT is a schema type "object"
	OBJECT SchemaType = iota
	// STRING is a schema type "string"
	STRING SchemaType = iota
)

var SchemaTypes = map[string]SchemaType{
	"array":   ARRAY,
	"boolean": BOOLEAN,
	"integer": INTEGER,
	"number":  NUMBER,
	"null":    NULL,
	"object":  OBJECT,
	"string":  STRING,
}

// UnmarshalJSON for SchemaType so we can parse the schema
// type string and set the ENUM
func (s *SchemaType) UnmarshalJSON(b []byte) error {
	var schemaType string
	err := json.Unmarshal(b, &schemaType)
	if err != nil {
		return err
	}
	if val, ok := SchemaTypes[schemaType]; ok {
		*s = val
		return nil
	}
	return fmt.Errorf("unknown schema type \"%s\"", schemaType)
}

// MarshalJSON for SchemaType so we serialized the schema back
// to json for debugging
func (s *SchemaType) MarshalJSON() ([]byte, error) {
	schemaType := s.String()
	if schemaType == "unknown" {
		return nil, fmt.Errorf("unknown Schema Type: %#v", s)
	}
	return []byte(fmt.Sprintf("%q", schemaType)), nil
}

func (s SchemaType) String() string {
	switch s {
	case ANY:
		return "any"
	case ARRAY:
		return "array"
	case BOOLEAN:
		return "boolean"
	case INTEGER:
		return "integer"
	case NUMBER:
		return "number"
	case NULL:
		return "null"
	case OBJECT:
		return "object"
	case STRING:
		return "string"
	}
	return "unknown"
}

// SchemaProcessor object used to convert json schemas to golang structs
type SchemaProcessor struct {
	outputDir    string
	packageName  string
	overwrite    bool
	stdout       bool
	format       bool
	comment      bool
	stdio        Stdio
	replacements map[string]string
	processed    map[string]bool
}

type SchemaProcessorOption func(*SchemaProcessor)

func OutputDir(dir string) SchemaProcessorOption {
	return func(s *SchemaProcessor) {
		s.outputDir = dir
	}
}

func PackageName(name string) SchemaProcessorOption {
	return func(s *SchemaProcessor) {
		s.packageName = name
	}
}

func Overwrite(overwrite bool) SchemaProcessorOption {
	return func(s *SchemaProcessor) {
		s.overwrite = overwrite
	}
}

func Stdout(stdout bool) SchemaProcessorOption {
	return func(s *SchemaProcessor) {
		s.stdout = stdout
	}
}

func Format(format bool) SchemaProcessorOption {
	return func(s *SchemaProcessor) {
		s.format = format
	}
}

func Comment(writeComments bool) SchemaProcessorOption {
	return func(s *SchemaProcessor) {
		s.comment = writeComments
	}
}

func IO(stdio Stdio) SchemaProcessorOption {
	return func(s *SchemaProcessor) {
		s.stdio = stdio
	}
}

func Replacements(replacements map[string]string) SchemaProcessorOption {
	return func(s *SchemaProcessor) {
		s.replacements = replacements
	}
}

func NewSchemaProcessor(options ...SchemaProcessorOption) *SchemaProcessor {
	s := &SchemaProcessor{
		stdio: Stdio{
			Stdin:  os.Stdin,
			Stdout: os.Stdout,
			Stderr: os.Stderr,
		},
	}

	for _, option := range options {
		option(s)
	}

	// Non-negotiable replacements.
	for k, v := range defaultReplacements {
		s.replacements[k] = v
	}

	return s
}

// Process will read a list of json schema files, parse them
// and write them to the OutputDir
func (s *SchemaProcessor) Process(files []string) error {
	for _, file := range files {
		var r io.Reader
		var b []byte
		if file == "-" {
			r = s.stdio.Stdin
		} else {
			fh, err := os.OpenFile(file, os.O_RDONLY, 0o644)
			defer fh.Close()
			if err != nil {
				return err
			}
			r = fh
		}
		b, err := io.ReadAll(r)
		if err != nil {
			return err
		}

		schema, err := s.ParseSchema(b)
		if err != nil {
			return err
		}

		_, err = s.processSchema(schema)
		if err != nil {
			return err
		}
	}
	return nil
}

// ParseSchema simply parses the schema and post-processes the objects
// so each knows the Root object and also resolve/flatten any $ref objects
// found in the document.
func (s *SchemaProcessor) ParseSchema(data []byte) (*Schema, error) {
	schema := &Schema{}
	err := json.Unmarshal(data, schema)
	if err != nil {
		return nil, err
	}

	raw := map[string]any{}
	err = json.Unmarshal(data, &raw)
	if err != nil {
		return nil, err
	}
	schema.raw = raw

	setRoot(schema, schema)
	return schema, nil
}

func (s *SchemaProcessor) structComment(schema *Schema, typeName string) string {
	if !s.comment {
		return ""
	}
	prettySchema, _ := json.MarshalIndent(schema, "// ", "  ")
	return fmt.Sprintf("// %s defined from schema:\n// %s\n", typeName, prettySchema)
}

func (s *SchemaProcessor) processSchema(schema *Schema) (typeName string, err error) {
	switch schema.Type {
	case OBJECT:
		typeName = s.toCamel(schema.Name())
		switch {
		case schema.Properties != nil:
			typeData := fmt.Sprintf("%stype %s struct {\n", s.structComment(schema, typeName), typeName)
			keys := []string{}
			for k := range schema.Properties {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				v := schema.Properties[k]
				subTypeName, err := s.processSchema(v)
				if err != nil {
					return "", err
				}
				typeData += fmt.Sprintf("    %s %s `json:\"%s,omitempty\" yaml:\"%s,omitempty\"`\n", s.toCamel(k), subTypeName, k, k)
			}
			typeData += "}\n\n"
			if err := s.writeGoCode(typeName, typeData); err != nil {
				return "", err
			}
			typeName = fmt.Sprintf("*%s", typeName)
		case schema.PatternProperties != nil:
			keys := []string{}
			for k := range schema.PatternProperties {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				v := schema.PatternProperties[k]
				subTypeName, err := s.processSchema(v)
				if err != nil {
					return "", err
				}

				// verify subTypeName is not a simple type
				if caser.String(subTypeName) == subTypeName {
					typeName = strings.TrimPrefix(fmt.Sprintf("%sMap", subTypeName), "*")
					typeData := fmt.Sprintf("%stype %s map[string]%s\n\n", s.structComment(schema, typeName), typeName, subTypeName)
					if err := s.writeGoCode(typeName, typeData); err != nil {
						return "", err
					}
				} else {
					typeName = fmt.Sprintf("map[string]%s", subTypeName)
				}
			}
		case schema.AdditionalProperties:
			// TODO we can probably do better, but this is a catch-all for now
			typeName = "map[string]any"
		}
	case ARRAY:
		subTypeName, err := s.processSchema(schema.Items)
		if err != nil {
			return "", err
		}

		typeName = s.toCamel(schema.Name())
		if typeName == "" {
			if caser.String(subTypeName) == subTypeName {
				if strings.HasSuffix(subTypeName, "s") {
					typeName = fmt.Sprintf("%ses", subTypeName)
				} else {
					typeName = fmt.Sprintf("%ss", subTypeName)
				}
			}
		}
		if typeName != "" {
			typeName = strings.TrimPrefix(typeName, "*")
			typeData := fmt.Sprintf("%stype %s []%s\n\n", s.structComment(schema, typeName), typeName, subTypeName)
			if err := s.writeGoCode(typeName, typeData); err != nil {
				return "", err
			}
		} else {
			typeName = fmt.Sprintf("[]%s", subTypeName)
		}
	case ANY:
		switch {
		case len(schema.OneOf) > 0:
			return s.mergeSchemas(schema, schema.OneOf...)
		case schema.Const != "":
			// Const is a special case of Enum
			return "string", nil
		case len(schema.Enum) > 0:
			// TODO this is bogus, but assuming Enums are string types for now
			return "string", nil
		}
		typeName = "any"
	case BOOLEAN:
		typeName = "bool"
	case INTEGER:
		typeName = "int"
	case NUMBER:
		typeName = "float64"
	case NULL:
		typeName = "any"
	case STRING:
		typeName = "string"
	}
	return
}

func (s *SchemaProcessor) mergeSchemas(parent *Schema, schemas ...*Schema) (typeName string, err error) {
	switch len(schemas) {
	case 0:
		return "", fmt.Errorf("merging zero schemas")
	case 1:
		// TODO: Not sure this is correct, should the name come from the oneOf
		// schema or the only constraint schema?
		return s.processSchema(schemas[0])
	}

	mergedParent := &Schema{
		Description: parent.Name(),
		Root:        parent.Root,
		Properties:  map[string]*Schema{},
		Type:        OBJECT,
	}

	uncommonSchemas := map[string]*Schema{}
	for _, schema := range schemas {
		// TODO we need a Schema.Copy() function
		uncommonSchemas[schema.Name()] = &Schema{
			Description: schema.Name(),
			Root:        parent.Root,
			Properties:  map[string]*Schema{},
			Type:        schema.Type,
		}
	}

	// find any common properties, and assign them to mergeParent
	// else create subtype with uncommon properties with `json:",inline"`

	allProperties := map[string]int{}
	for _, schema := range schemas {
		for p := range schema.Properties {
			allProperties[p]++
		}
	}

	for _, schema := range schemas {
		for p, v := range schema.Properties {
			if allProperties[p] > 1 {
				mergedParent.Properties[p] = v
			} else {
				uncommonSchemas[schema.Name()].Properties[p] = v
			}
		}
	}

	typeName = s.toCamel(mergedParent.Name())
	typeData := fmt.Sprintf("%stype %s struct {\n", s.structComment(mergedParent, typeName), typeName)

	keys := []string{}
	for k := range mergedParent.Properties {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := mergedParent.Properties[k]
		subTypeName, err := s.processSchema(v)
		if err != nil {
			return "", err
		}
		typeData += fmt.Sprintf("    %s %s `json:\"%s,omitempty\" yaml:\"%s,omitempty\"`\n", s.toCamel(k), subTypeName, k, k)
	}

	oneOfKeys := []string{}
	for name, schema := range uncommonSchemas {
		if len(schema.Properties) > 0 {
			oneOfKeys = append(oneOfKeys, name)
		}
	}
	sort.Strings(oneOfKeys)

	for _, k := range oneOfKeys {
		oneOfTypeName, err := s.processSchema(uncommonSchemas[k])
		if err != nil {
			return "", err
		}
		typeData += fmt.Sprintf("    %s %s `json:\",inline\" yaml:\",inline\"`\n", s.toCamel(k), oneOfTypeName)
	}

	typeData += "}\n\n"
	if err := s.writeGoCode(typeName, typeData); err != nil {
		return "", err
	}
	return typeName, nil
}

func (s *SchemaProcessor) writeGoCode(typeName, code string) error {
	if seen, ok := s.processed[typeName]; ok && seen {
		return nil
	}
	// mark schemas as processed so we dont print/write it out again
	if s.processed == nil {
		s.processed = map[string]bool{
			typeName: true,
		}
	} else {
		s.processed[typeName] = true
	}

	if s.stdout {
		if s.format {
			cmd := exec.Command("gofmt", "-s")
			inPipe, _ := cmd.StdinPipe()
			cmd.Stdout = s.stdio.Stdout
			cmd.Stderr = s.stdio.Stderr
			cmd.Start()
			inPipe.Write([]byte(code))
			inPipe.Close()
			return cmd.Wait()
		}
		fmt.Print(code)
		return nil
	}
	file := path.Join(s.outputDir, fmt.Sprintf("%s.go", strcase.ToSnakeWithIgnore(typeName, "Id")))
	if !s.overwrite {
		if _, err := os.Stat(file); err == nil {
			log.Printf("File %s already exists, skipping without -overwrite", file)
			return nil
		}
	}
	fh, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer fh.Close()
	preamble := "// Code generated by github.com/rdeusser/slipscheme DO NOT EDIT.\n"
	preamble += fmt.Sprintf("package %s\n", s.packageName)
	fmt.Printf("Writing %s\n", file)

	if _, err := fh.Write([]byte(preamble)); err != nil {
		return err
	}
	if _, err := fh.Write([]byte(code)); err != nil {
		return err
	}

	if s.format {
		cmd := exec.Command("gofmt", "-s", "-w", file)
		cmd.Stdin = s.stdio.Stdin
		cmd.Stdout = s.stdio.Stdout
		cmd.Stderr = s.stdio.Stderr
		return cmd.Run()
	}

	return nil
}

func (s *SchemaProcessor) toCamel(str string) string {
	word := strcase.ToCamel(str)

	for k, v := range s.replacements {
		if strings.HasSuffix(word, k) {
			return strings.TrimSuffix(word, k) + v
		}
	}

	for k, v := range s.replacements {
		if strings.HasPrefix(word, k) {
			word = v + strings.TrimPrefix(word, k)
		}
	}

	return word
}

func setRoot(root, schema *Schema) {
	schema.Root = root
	if schema.Properties != nil {
		for k, v := range schema.Properties {
			setRoot(root, v)
			if v.Name() == "" {
				v.Title = k
			}
		}
	}
	if schema.PatternProperties != nil {
		for _, v := range schema.PatternProperties {
			setRoot(root, v)
		}
	}
	if schema.Items != nil {
		setRoot(root, schema.Items)
	}

	for _, one := range schema.OneOf {
		setRoot(root, one)
	}

	if schema.Ref != "" {
		schemaPath := strings.Split(schema.Ref, "/")
		var ctx any
		ctx = schema
		for _, part := range schemaPath {
			switch part {
			case "#":
				ctx = root
			case "definitions":
				ctx = ctx.(*Schema).Definitions
			case "properties":
				ctx = ctx.(*Schema).Properties
			case "patternProperties":
				ctx = ctx.(*Schema).PatternProperties
			case "items":
				ctx = ctx.(*Schema).Items
			default:
				if cast, ok := ctx.(map[string]*Schema); ok {
					if def, ok := cast[part]; ok {
						ctx = def
						continue
					}
				}
				// no match in the structure, so loop through the raw document
				// in case they are using out-of-spec paths ie #/$special/thing
				var cursor any = root.raw
				for _, part := range schemaPath {
					if part == "#" {
						continue
					}
					cast, ok := cursor.(map[string]any)
					if !ok {
						panic(fmt.Sprintf("Expected map[string]any, got: %T at path %q in $ref %q", cursor, part, schema.Ref))
					}
					value, ok := cast[part]
					if !ok {
						panic(fmt.Sprintf("path %q for $ref %q not found in document %#v", part, schema.Ref, cast))
					}
					cursor = value
				}

				// turn it back into json
				document, err := json.Marshal(cursor)
				if err != nil {
					panic(err)
				}
				// now try to parse the new extracted sub document as a schema
				refSchema := &Schema{}
				err = json.Unmarshal(document, refSchema)
				if err != nil {
					panic(err)
				}
				setRoot(root, refSchema)

				if refSchema.Name() == "" {
					// just guess on the name from the json document path
					refSchema.Description = part
				}
				ctx = refSchema
			}
		}
		if cast, ok := ctx.(*Schema); ok {
			*schema = *cast
		}
	}
}
