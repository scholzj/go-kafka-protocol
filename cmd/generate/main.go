package main

import (
	"encoding/json"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// JSON structures
type MessageDef struct {
	ApiKey           int16   `json:"apiKey"`
	Type             string  `json:"type"` // "request", "response", "header"
	Name             string  `json:"name"`
	ValidVersions    string  `json:"validVersions"`
	FlexibleVersions string  `json:"flexibleVersions"`
	Fields           []Field `json:"fields"`
	CommonStructs    []Field `json:"commonStructs"` // Shared structs
}

type Field struct {
	Name             string      `json:"name"`
	Type             string      `json:"type"`
	Versions         string      `json:"versions"`
	NullableVersions string      `json:"nullableVersions"`
	Tag              *int        `json:"tag"`
	TaggedVersions   string      `json:"taggedVersions"`
	Default          interface{} `json:"default"`   // Can be string, number, or bool
	Ignorable        interface{} `json:"ignorable"` // Can be string or bool
	MapKey           bool        `json:"mapKey"`
	Fields           []Field     `json:"fields"` // For struct/array types
	EntityType       string      `json:"entityType"`
	About            string      `json:"about"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <sources-dir> [output-dir]\n", os.Args[0])
		os.Exit(1)
	}

	sourcesDir := os.Args[1]
	outputDir := "api"
	if len(os.Args) > 2 {
		outputDir = os.Args[2]
	}

	// Create output directory
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output directory: %v\n", err)
		os.Exit(1)
	}

	// Find all JSON files
	jsonFiles, err := filepath.Glob(filepath.Join(sourcesDir, "*.json"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error finding JSON files: %v\n", err)
		os.Exit(1)
	}

	// Parse header version info
	headerVersions := make(map[string]int)
	for _, file := range jsonFiles {
		if strings.HasSuffix(file, "RequestHeader.json") || strings.HasSuffix(file, "ResponseHeader.json") {
			headerVersions = parseHeaderVersions(file)
			break
		}
	}

	// Group request/response pairs together
	messagePairs := make(map[string][]*MessageDef) // Base name -> []MessageDef

	// First pass: collect all messages
	for _, file := range jsonFiles {
		baseName := filepath.Base(file)
		if strings.HasSuffix(baseName, "Header.json") {
			continue // Skip header files, we'll handle them separately
		}

		def, err := parseJSONFile(file)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing %s: %v\n", file, err)
			continue
		}

		if def.Type != "request" && def.Type != "response" {
			continue
		}

		// Extract base name (remove Request/Response suffix)
		baseName = strings.TrimSuffix(def.Name, "Request")
		baseName = strings.TrimSuffix(baseName, "Response")

		if messagePairs[baseName] == nil {
			messagePairs[baseName] = make([]*MessageDef, 0, 2)
		}
		messagePairs[baseName] = append(messagePairs[baseName], def)
	}

	// Second pass: generate files in subdirectories
	for baseName, defs := range messagePairs {
		// Create subdirectory for this message pair
		packageName := strings.ToLower(baseName)
		subDir := filepath.Join(outputDir, packageName)
		if err := os.MkdirAll(subDir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "Error creating subdirectory %s: %v\n", subDir, err)
			continue
		}

		// Collect all common structs from all files in this package
		allCommonStructs := make(map[string]Field) // struct name -> Field
		for _, def := range defs {
			for _, commonStruct := range def.CommonStructs {
				structName := toExportedName(commonStruct.Name)
				if structName != "" {
					// Use the first definition found (they should be identical)
					if _, exists := allCommonStructs[structName]; !exists {
						allCommonStructs[structName] = commonStruct
					}
				}
			}
		}

		// Generate each message in the same package
		for i, def := range defs {
			// Generate helpers only in the first file
			generateHelpers := (i == 0)
			// Convert map to slice for common structs
			commonStructsSlice := make([]Field, 0, len(allCommonStructs))
			for _, cs := range allCommonStructs {
				commonStructsSlice = append(commonStructsSlice, cs)
			}
			// Replace def.CommonStructs with collected common structs
			def.CommonStructs = commonStructsSlice
			if err := generateGoFileInPackage(subDir, def, headerVersions, packageName, generateHelpers); err != nil {
				fmt.Fprintf(os.Stderr, "Error generating %s: %v\n", def.Name, err)
				continue
			}
		}
	}

	fmt.Printf("Generated API files in %s/\n", outputDir)
}

func parseHeaderVersions(file string) map[string]int {
	data, err := os.ReadFile(file)
	if err != nil {
		return make(map[string]int)
	}

	var def MessageDef
	if err := json.Unmarshal(data, &def); err != nil {
		return make(map[string]int)
	}

	versions := parseVersionRange(def.ValidVersions)
	result := make(map[string]int)
	if len(versions) > 0 {
		// Use the highest version as the header version
		result[def.Type] = versions[len(versions)-1]
	}
	return result
}

func parseJSONFile(file string) (*MessageDef, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}

	// Remove comments (simple approach - lines starting with //)
	lines := strings.Split(string(data), "\n")
	var cleanedLines []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		// Skip comment-only lines, but keep lines with code before comments
		if !strings.HasPrefix(trimmed, "//") && trimmed != "" {
			// Remove inline comments
			if idx := strings.Index(line, "//"); idx >= 0 {
				line = line[:idx]
			}
			cleanedLines = append(cleanedLines, strings.TrimRight(line, " \t"))
		}
	}
	cleanedData := strings.Join(cleanedLines, "\n")

	var def MessageDef
	if err := json.Unmarshal([]byte(cleanedData), &def); err != nil {
		return nil, fmt.Errorf("JSON unmarshal error: %w", err)
	}

	return &def, nil
}

func generateGoFile(outputDir string, def *MessageDef, headerVersions map[string]int) error {
	return generateGoFileInPackage(outputDir, def, headerVersions, "", true)
}

func generateGoFileInPackage(outputDir string, def *MessageDef, headerVersions map[string]int, packageName string, generateHelpers bool) error {
	fileName := strings.ToLower(def.Name) + ".go"
	filePath := filepath.Join(outputDir, fileName)

	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Determine header version
	headerVersion := 1
	if def.Type == "request" {
		if v, ok := headerVersions["request"]; ok {
			headerVersion = v
		}
	} else if def.Type == "response" {
		if v, ok := headerVersions["response"]; ok {
			headerVersion = v
		}
	}

	// Parse versions
	validVersions := parseVersionRange(def.ValidVersions)
	flexibleVersions := parseVersionRange(def.FlexibleVersions)

	// Generate code
	code := generateCode(def, validVersions, flexibleVersions, headerVersion, packageName, generateHelpers)

	// Format the code
	formatted, err := format.Source([]byte(code))
	if err != nil {
		// If formatting fails, write unformatted code
		fmt.Fprintf(os.Stderr, "Warning: Could not format %s: %v\n", fileName, err)
		formatted = []byte(code)
	}

	_, err = f.Write(formatted)
	return err
}

func generateCode(def *MessageDef, validVersions []int, flexibleVersions []int, headerVersion int, packageName string, generateHelpers bool) string {
	var buf strings.Builder

	// Package declaration
	buf.WriteString(fmt.Sprintf("package %s\n\n", packageName))

	// Check if UUID types are used
	usesUUID := hasUUIDType(def.Fields) || hasUUIDTypeInCommonStructs(def.CommonStructs)

	// Imports
	buf.WriteString("import (\n")
	buf.WriteString("\t\"bytes\"\n")
	buf.WriteString("\t\"errors\"\n")
	buf.WriteString("\t\"io\"\n\n")
	if usesUUID {
		buf.WriteString("\t\"github.com/google/uuid\"\n")
	}
	buf.WriteString("\t\"github.com/scholzj/go-kafka-protocol/protocol\"\n")
	buf.WriteString(")\n\n")

	// Constants
	buf.WriteString(fmt.Sprintf("const (\n"))
	buf.WriteString(fmt.Sprintf("\t%sApiKey = %d\n", def.Name, def.ApiKey))
	buf.WriteString(fmt.Sprintf("\t%sHeaderVersion = %d\n", def.Name, headerVersion))
	buf.WriteString(")\n\n")

	// Struct definition
	buf.WriteString(fmt.Sprintf("// %s represents a %s message.\n", def.Name, def.Type))
	buf.WriteString(fmt.Sprintf("type %s struct {\n", def.Name))

	// Track nested struct types first (needed for both struct fields and nested struct generation)
	nestedStructTypes := make(map[string]bool)
	collectNestedStructTypes(def.Fields, nestedStructTypes)

	// Also collect common struct names to prevent duplicate generation
	commonStructNames := make(map[string]bool)
	for _, commonStruct := range def.CommonStructs {
		structName := toExportedName(commonStruct.Name)
		if structName != "" {
			commonStructNames[structName] = true
		}
	}

	// Generate struct fields
	generateStructFieldsWithTypes(&buf, def.Fields, 1, validVersions, flexibleVersions, "", def.Name, nestedStructTypes)

	buf.WriteString("}\n\n")

	// Generate Encode method
	generateEncodeMethod(&buf, def, validVersions, flexibleVersions)

	// Generate Decode method
	generateDecodeMethod(&buf, def, validVersions, flexibleVersions)

	// Generate Write method
	generateWriteMethod(&buf, def, validVersions, flexibleVersions, nestedStructTypes)

	// Generate Read method
	generateReadMethod(&buf, def, validVersions, flexibleVersions, nestedStructTypes)

	// Generate helper structs for nested types (reuse nestedStructTypes from above)
	// Track which struct names have been generated (including nested ones and common structs)
	generatedStructNames := make(map[string]bool)
	// Pre-populate with common struct names to prevent generating them as nested structs
	// (but we'll still generate them as common structs below)
	for name := range commonStructNames {
		generatedStructNames[name] = true
	}
	generateNestedStructsWithTypesAndTracking(&buf, def.Fields, validVersions, flexibleVersions, def.Name, nestedStructTypes, generatedStructNames)

	// Generate common structs (these are shared and not prefixed)
	// Only generate common structs in the first file of a package to avoid redeclaration
	if generateHelpers {
		for _, commonStruct := range def.CommonStructs {
			structName := toExportedName(commonStruct.Name)
			if structName != "" && len(commonStruct.Fields) > 0 {
				// Always generate common structs (pre-population was only to prevent nested generation)
				// But check if it was already generated as a nested struct (shouldn't happen, but check anyway)
				if !generatedStructNames[structName] {
					generatedStructNames[structName] = true
				}
				// Generate the struct (even if already in generatedStructNames, we need to output it)
				buf.WriteString(fmt.Sprintf("// %s represents %s.\n", structName, commonStruct.About))
				buf.WriteString(fmt.Sprintf("type %s struct {\n", structName))
				// Common structs don't use parent prefix, so pass empty nestedStructTypes
				commonNestedTypes := make(map[string]bool)
				collectNestedStructTypes(commonStruct.Fields, commonNestedTypes)
				generateStructFieldsWithTypes(&buf, commonStruct.Fields, 1, validVersions, flexibleVersions, "", "", commonNestedTypes)
				buf.WriteString("}\n\n")
				// Recursively generate nested structs in common structs
				generateNestedStructsWithTypesAndTracking(&buf, commonStruct.Fields, validVersions, flexibleVersions, "", commonNestedTypes, generatedStructNames)
			}
		}
	}

	// Generate tagged fields methods (always generate, even if empty, for flexible versions)
	if len(flexibleVersions) > 0 {
		generateTaggedFieldsMethods(&buf, def, validVersions, flexibleVersions, nestedStructTypes, def.CommonStructs)
	}

	// Generate helper functions only if requested (first file in package)
	if generateHelpers {
		generateHelperFunctions(&buf)
	}

	return buf.String()
}

func generateHelperFunctions(buf *strings.Builder) {
	// No helper functions needed anymore - bool is handled directly via protocol.WriteBool/ReadBool
}

func hasTaggedFields(fields []Field) bool {
	for _, field := range fields {
		if field.Tag != nil {
			return true
		}
		if len(field.Fields) > 0 {
			if hasTaggedFields(field.Fields) {
				return true
			}
		}
	}
	return false
}

func generateStructFields(buf *strings.Builder, fields []Field, indent int, validVersions []int, flexibleVersions []int, prefix string, parentName string) {
	// Wrapper that collects nested types
	nestedStructTypes := make(map[string]bool)
	collectNestedStructTypes(fields, nestedStructTypes)
	generateStructFieldsWithTypes(buf, fields, indent, validVersions, flexibleVersions, prefix, parentName, nestedStructTypes)
}

func generateStructFieldsWithTypes(buf *strings.Builder, fields []Field, indent int, validVersions []int, flexibleVersions []int, prefix string, parentName string, nestedStructTypes map[string]bool) {
	indentStr := strings.Repeat("\t", indent)

	for _, field := range fields {
		fieldVersions := parseVersionRange(field.Versions)
		if len(fieldVersions) == 0 {
			continue
		}

		// Determine Go type
		goType := getGoType(field.Type, field.NullableVersions, validVersions, flexibleVersions, field.Tag != nil, parentName, nestedStructTypes)

		// Field name
		fieldName := toExportedName(field.Name)
		if prefix != "" {
			fieldName = prefix + fieldName
		}

		// Comment
		if field.About != "" {
			buf.WriteString(fmt.Sprintf("%s// %s\n", indentStr, field.About))
		}

		// Field definition
		buf.WriteString(fmt.Sprintf("%s%s %s", indentStr, fieldName, goType))

		// Tag with version info
		versionTag := formatVersionRange(fieldVersions)
		buf.WriteString(fmt.Sprintf(" `json:\"%s\" versions:\"%s\"", strings.ToLower(field.Name), versionTag))
		if field.Tag != nil {
			buf.WriteString(fmt.Sprintf(" tag:\"%d\"", *field.Tag))
		}
		buf.WriteString("`\n")

		// Handle nested structs (for array elements or nested structs)
		if len(field.Fields) > 0 && (strings.HasPrefix(field.Type, "[]") || field.Type == "struct") {
			// Nested structs will be generated separately
		}
	}
}

func generateNestedStructsWithTypes(buf *strings.Builder, fields []Field, validVersions []int, flexibleVersions []int, parentName string, nestedStructTypes map[string]bool) {
	generatedStructNames := make(map[string]bool)
	generateNestedStructsWithTypesAndTracking(buf, fields, validVersions, flexibleVersions, parentName, nestedStructTypes, generatedStructNames)
}

func generateNestedStructsWithTypesAndTracking(buf *strings.Builder, fields []Field, validVersions []int, flexibleVersions []int, parentName string, nestedStructTypes map[string]bool, generatedStructNames map[string]bool) {
	for _, field := range fields {
		if len(field.Fields) > 0 {
			structName := getStructName(field.Type, field.Name, parentName)
			if structName != "" && !generatedStructNames[structName] {
				generatedStructNames[structName] = true
				buf.WriteString(fmt.Sprintf("// %s represents %s.\n", structName, field.About))
				buf.WriteString(fmt.Sprintf("type %s struct {\n", structName))
				generateStructFieldsWithTypes(buf, field.Fields, 1, validVersions, flexibleVersions, "", parentName, nestedStructTypes)
				buf.WriteString("}\n\n")
			}

			// Recursively generate nested structs
			generateNestedStructsWithTypesAndTracking(buf, field.Fields, validVersions, flexibleVersions, parentName, nestedStructTypes, generatedStructNames)
		}
	}
}

func generateNestedStructs(buf *strings.Builder, fields []Field, validVersions []int, flexibleVersions []int, parentName string) {
	// This is a wrapper that collects nested types first
	nestedStructTypes := make(map[string]bool)
	collectNestedStructTypes(fields, nestedStructTypes)
	generateNestedStructsWithTypes(buf, fields, validVersions, flexibleVersions, parentName, nestedStructTypes)
}

func collectNestedStructTypes(fields []Field, result map[string]bool) {
	for _, field := range fields {
		if len(field.Fields) > 0 {
			// This is a nested struct
			baseName := toExportedName(field.Type)
			if strings.HasPrefix(field.Type, "[]") {
				baseName = toExportedName(strings.TrimPrefix(field.Type, "[]"))
			}
			if baseName != "" {
				result[baseName] = true
			}
			// Recursively collect nested types
			collectNestedStructTypes(field.Fields, result)
		}
	}
}

func generateEncodeMethod(buf *strings.Builder, def *MessageDef, validVersions []int, flexibleVersions []int) {
	buf.WriteString(fmt.Sprintf("// Encode encodes a %s to a byte slice for the given version.\n", def.Name))
	buf.WriteString(fmt.Sprintf("func (m *%s) Encode(version int16) ([]byte, error) {\n", def.Name))
	buf.WriteString("\tvar buf bytes.Buffer\n")
	buf.WriteString("\tif err := m.Write(&buf, version); err != nil {\n")
	buf.WriteString("\t\treturn nil, err\n")
	buf.WriteString("\t}\n")
	buf.WriteString("\treturn buf.Bytes(), nil\n")
	buf.WriteString("}\n\n")
}

func generateDecodeMethod(buf *strings.Builder, def *MessageDef, validVersions []int, flexibleVersions []int) {
	buf.WriteString(fmt.Sprintf("// Decode decodes a %s from a byte slice for the given version.\n", def.Name))
	buf.WriteString(fmt.Sprintf("func (m *%s) Decode(data []byte, version int16) error {\n", def.Name))
	buf.WriteString("\tr := bytes.NewReader(data)\n")
	buf.WriteString("\treturn m.Read(r, version)\n")
	buf.WriteString("}\n\n")
}

func generateWriteMethod(buf *strings.Builder, def *MessageDef, validVersions []int, flexibleVersions []int, nestedStructTypes map[string]bool) {
	buf.WriteString(fmt.Sprintf("// Write writes a %s to an io.Writer for the given version.\n", def.Name))
	buf.WriteString(fmt.Sprintf("func (m *%s) Write(w io.Writer, version int16) error {\n", def.Name))

	validVers := parseVersionRange(def.ValidVersions)
	if len(validVers) > 0 {
		buf.WriteString(fmt.Sprintf("\tif version < %d || version > %d {\n",
			validVers[0],
			validVers[len(validVers)-1]))
		buf.WriteString("\t\treturn errors.New(\"unsupported version\")\n")
		buf.WriteString("\t}\n\n")
	}

	buf.WriteString("\tisFlexible := false\n")
	if len(flexibleVersions) > 0 {
		buf.WriteString(fmt.Sprintf("\tif version >= %d {\n", flexibleVersions[0]))
		buf.WriteString("\t\tisFlexible = true\n")
		buf.WriteString("\t}\n\n")
	}

	// Write fields
	generateWriteFields(buf, def.Fields, 1, validVersions, flexibleVersions, "m", def.Name, nestedStructTypes, def.CommonStructs)

	// Write tagged fields if flexible
	if len(flexibleVersions) > 0 {
		buf.WriteString("\t// Write tagged fields if flexible\n")
		buf.WriteString("\tif isFlexible {\n")
		buf.WriteString("\t\tif err := m.writeTaggedFields(w, version); err != nil {\n")
		buf.WriteString("\t\t\treturn err\n")
		buf.WriteString("\t\t}\n")
		buf.WriteString("\t}\n")
	}

	buf.WriteString("\treturn nil\n")
	buf.WriteString("}\n\n")
}

func generateReadMethod(buf *strings.Builder, def *MessageDef, validVersions []int, flexibleVersions []int, nestedStructTypes map[string]bool) {
	buf.WriteString(fmt.Sprintf("// Read reads a %s from an io.Reader for the given version.\n", def.Name))
	buf.WriteString(fmt.Sprintf("func (m *%s) Read(r io.Reader, version int16) error {\n", def.Name))

	validVers := parseVersionRange(def.ValidVersions)
	if len(validVers) > 0 {
		buf.WriteString(fmt.Sprintf("\tif version < %d || version > %d {\n",
			validVers[0],
			validVers[len(validVers)-1]))
		buf.WriteString("\t\treturn errors.New(\"unsupported version\")\n")
		buf.WriteString("\t}\n\n")
	}

	buf.WriteString("\tisFlexible := false\n")
	if len(flexibleVersions) > 0 {
		buf.WriteString(fmt.Sprintf("\tif version >= %d {\n", flexibleVersions[0]))
		buf.WriteString("\t\tisFlexible = true\n")
		buf.WriteString("\t}\n\n")
	}

	// Read fields
	generateReadFields(buf, def.Fields, 1, validVersions, flexibleVersions, "m", def.Name, nestedStructTypes, def.CommonStructs)

	// Read tagged fields if flexible
	if len(flexibleVersions) > 0 {
		buf.WriteString("\t// Read tagged fields if flexible\n")
		buf.WriteString("\tif isFlexible {\n")
		buf.WriteString("\t\tif err := m.readTaggedFields(r, version); err != nil {\n")
		buf.WriteString("\t\t\treturn err\n")
		buf.WriteString("\t\t}\n")
		buf.WriteString("\t}\n")
	}

	buf.WriteString("\treturn nil\n")
	buf.WriteString("}\n\n")
}

// Helper functions
func parseVersionRange(versionStr string) []int {
	if versionStr == "" {
		return []int{}
	}

	parts := strings.Split(versionStr, "-")
	if len(parts) == 1 {
		// Single version or range like "3+"
		if strings.HasSuffix(parts[0], "+") {
			start, _ := strconv.Atoi(strings.TrimSuffix(parts[0], "+"))
			// Return a large range (we'll handle this in code generation)
			return []int{start, 999}
		}
		v, _ := strconv.Atoi(parts[0])
		return []int{v}
	}

	start, _ := strconv.Atoi(parts[0])
	end, _ := strconv.Atoi(parts[1])

	var versions []int
	for i := start; i <= end; i++ {
		versions = append(versions, i)
	}
	return versions
}

func formatVersionRange(versions []int) string {
	if len(versions) == 0 {
		return ""
	}
	if len(versions) == 1 {
		return strconv.Itoa(versions[0])
	}
	return fmt.Sprintf("%d-%d", versions[0], versions[len(versions)-1])
}

func isFlexibleVersion(version int, flexibleVersions []int) bool {
	if len(flexibleVersions) == 0 {
		return false
	}
	minFlexible := flexibleVersions[0]
	return version >= minFlexible
}

func getGoType(fieldType string, nullableVersions string, validVersions []int, flexibleVersions []int, isTagged bool, parentName string, nestedStructTypes map[string]bool) string {
	isNullable := nullableVersions != ""
	isFlexible := len(flexibleVersions) > 0 && isFlexibleVersion(validVersions[0], flexibleVersions)

	// Handle arrays
	if strings.HasPrefix(fieldType, "[]") {
		elemType := strings.TrimPrefix(fieldType, "[]")
		goElemType := getGoType(elemType, "", validVersions, flexibleVersions, false, parentName, nestedStructTypes)
		return "[]" + goElemType
	}

	// Map primitive types
	switch fieldType {
	case "bool":
		return "bool"
	case "int8":
		return "int8"
	case "int16":
		return "int16"
	case "uint16":
		return "uint16"
	case "int32":
		return "int32"
	case "uint32":
		return "uint32"
	case "int64":
		return "int64"
	case "float64":
		return "float64"
	case "string":
		if isFlexible && !isTagged {
			if isNullable {
				return "*string"
			}
			return "string"
		}
		if isNullable {
			return "*string"
		}
		return "string"
	case "uuid":
		return "uuid.UUID"
	case "bytes":
		if isFlexible && !isTagged {
			if isNullable {
				return "*[]byte"
			}
			return "[]byte"
		}
		if isNullable {
			return "*[]byte"
		}
		return "[]byte"
	case "records":
		if isFlexible {
			return "*[]byte"
		}
		return "*[]byte"
	default:
		// Struct type - check if it's nested or common
		baseName := toExportedName(fieldType)
		if baseName == "" {
			return "interface{}" // Fallback for unknown types
		}
		// If it's a nested struct in this message, use prefixed name
		// Otherwise, it's a common struct, use base name
		if nestedStructTypes[baseName] {
			return parentName + baseName
		}
		return baseName
	}
}

func getStructName(fieldType string, fieldName string, parentName string) string {
	var baseName string
	if strings.HasPrefix(fieldType, "[]") {
		elemType := strings.TrimPrefix(fieldType, "[]")
		baseName = toExportedName(elemType)
	} else {
		baseName = toExportedName(fieldType)
	}
	// If baseName is empty, use fieldName
	if baseName == "" {
		baseName = toExportedName(fieldName)
	}
	// Prefix with parent name to avoid conflicts across files
	return parentName + baseName
}

func toExportedName(name string) string {
	if name == "" {
		return ""
	}
	return strings.ToUpper(name[:1]) + name[1:]
}

// hasUUIDType checks if any field in the given fields uses UUID type
func hasUUIDType(fields []Field) bool {
	for _, field := range fields {
		if field.Type == "uuid" {
			return true
		}
		if strings.HasPrefix(field.Type, "[]") {
			elemType := strings.TrimPrefix(field.Type, "[]")
			if elemType == "uuid" {
				return true
			}
		}
		// Recursively check nested fields
		if len(field.Fields) > 0 {
			if hasUUIDType(field.Fields) {
				return true
			}
		}
	}
	return false
}

// hasUUIDTypeInCommonStructs checks if any common struct uses UUID type
func hasUUIDTypeInCommonStructs(commonStructs []Field) bool {
	for _, commonStruct := range commonStructs {
		if hasUUIDType(commonStruct.Fields) {
			return true
		}
	}
	return false
}

// hasStringType checks if any field in the given fields uses string type
func hasStringType(fields []Field) bool {
	for _, field := range fields {
		if field.Type == "string" {
			return true
		}
		if strings.HasPrefix(field.Type, "[]") {
			elemType := strings.TrimPrefix(field.Type, "[]")
			if elemType == "string" {
				return true
			}
		}
		// Recursively check nested fields
		if len(field.Fields) > 0 {
			if hasStringType(field.Fields) {
				return true
			}
		}
	}
	return false
}

// findCommonStruct finds a common struct by its name (case-insensitive)
func findCommonStruct(name string, commonStructs []Field) *Field {
	exportedName := toExportedName(name)
	for i := range commonStructs {
		if toExportedName(commonStructs[i].Name) == exportedName {
			return &commonStructs[i]
		}
	}
	return nil
}

// needsLoopIndex checks if the loop index is needed for array element generation
func needsLoopIndex(elemType string, field Field, commonStructs []Field) bool {
	// Primitive types don't need the index
	switch elemType {
	case "bool", "int8", "int16", "int32", "int64", "uint16", "uint32", "float64", "string", "uuid":
		return false
	}
	// For struct types, check if there are fields to write
	if len(field.Fields) > 0 {
		return true
	}
	// Check if it's a common struct
	commonStruct := findCommonStruct(elemType, commonStructs)
	if commonStruct != nil && len(commonStruct.Fields) > 0 {
		return true
	}
	return false
}

func generateWriteFields(buf *strings.Builder, fields []Field, indent int, validVersions []int, flexibleVersions []int, prefix string, parentName string, nestedStructTypes map[string]bool, commonStructs []Field) {
	indentStr := strings.Repeat("\t", indent)

	for _, field := range fields {
		fieldVersions := parseVersionRange(field.Versions)
		if len(fieldVersions) == 0 {
			continue
		}

		fieldName := toExportedName(field.Name)
		fieldPath := prefix + "." + fieldName

		// Check if field should be written for this version
		buf.WriteString(fmt.Sprintf("%s// %s\n", indentStr, field.Name))
		maxVersion := fieldVersions[len(fieldVersions)-1]
		if maxVersion == 999 {
			maxVersion = 999 // Use a large number for open-ended ranges
		}
		buf.WriteString(fmt.Sprintf("%sif version >= %d && version <= %d {\n",
			indentStr, fieldVersions[0], maxVersion))

		// Generate write code based on type
		generateFieldWrite(buf, field, fieldPath, indent+1, parentName, nestedStructTypes, commonStructs)

		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	}
}

func generateFieldWrite(buf *strings.Builder, field Field, fieldPath string, indent int, parentName string, nestedStructTypes map[string]bool, commonStructs []Field) {
	indentStr := strings.Repeat("\t", indent)

	// Handle tagged fields separately
	if field.Tag != nil {
		return // Tagged fields handled separately
	}

	switch {
	case strings.HasPrefix(field.Type, "[]"):
		// Array type - get the resolved Go type for the element
		elemType := strings.TrimPrefix(field.Type, "[]")
		// Use getGoType to resolve the element type properly (handles nested structs, common structs, etc.)
		// We need parent name and nested struct types - get them from the field's context
		// For now, use a simple approach: if it's a struct type, it should be prefixed
		generateArrayWrite(buf, field, fieldPath, elemType, indent, parentName, nestedStructTypes, commonStructs)
	case field.Type == "bool":
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteBool(w, %s); err != nil {\n", indentStr, fieldPath))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	case field.Type == "int8":
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteInt8(w, %s); err != nil {\n", indentStr, fieldPath))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	case field.Type == "int16":
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteInt16(w, %s); err != nil {\n", indentStr, fieldPath))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	case field.Type == "int32":
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteInt32(w, %s); err != nil {\n", indentStr, fieldPath))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	case field.Type == "int64":
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteInt64(w, %s); err != nil {\n", indentStr, fieldPath))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	case field.Type == "string":
		buf.WriteString(fmt.Sprintf("%sif isFlexible {\n", indentStr))
		if field.NullableVersions != "" {
			buf.WriteString(fmt.Sprintf("%s\tif err := protocol.WriteCompactNullableString(w, %s); err != nil {\n", indentStr, fieldPath))
		} else {
			buf.WriteString(fmt.Sprintf("%s\tif err := protocol.WriteCompactString(w, %s); err != nil {\n", indentStr, fieldPath))
		}
		buf.WriteString(fmt.Sprintf("%s\t\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s} else {\n", indentStr))
		if field.NullableVersions != "" {
			buf.WriteString(fmt.Sprintf("%s\tif err := protocol.WriteNullableString(w, %s); err != nil {\n", indentStr, fieldPath))
		} else {
			buf.WriteString(fmt.Sprintf("%s\tif err := protocol.WriteString(w, %s); err != nil {\n", indentStr, fieldPath))
		}
		buf.WriteString(fmt.Sprintf("%s\t\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	case field.Type == "uuid":
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteUUID(w, %s); err != nil {\n", indentStr, fieldPath))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	case field.Type == "bytes" || field.Type == "records":
		// records type is always nullable (*[]byte), bytes can be nullable based on nullableVersions
		isNullable := field.Type == "records" || field.NullableVersions != ""
		buf.WriteString(fmt.Sprintf("%sif isFlexible {\n", indentStr))
		if isNullable {
			buf.WriteString(fmt.Sprintf("%s\tif err := protocol.WriteCompactNullableBytes(w, %s); err != nil {\n", indentStr, fieldPath))
		} else {
			buf.WriteString(fmt.Sprintf("%s\tif err := protocol.WriteCompactBytes(w, %s); err != nil {\n", indentStr, fieldPath))
		}
		buf.WriteString(fmt.Sprintf("%s\t\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s} else {\n", indentStr))
		if isNullable {
			buf.WriteString(fmt.Sprintf("%s\tif err := protocol.WriteNullableBytes(w, %s); err != nil {\n", indentStr, fieldPath))
		} else {
			buf.WriteString(fmt.Sprintf("%s\tif err := protocol.WriteBytes(w, %s); err != nil {\n", indentStr, fieldPath))
		}
		buf.WriteString(fmt.Sprintf("%s\t\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	default:
		// Struct type - recursive write
		if len(field.Fields) > 0 {
			generateStructWrite(buf, field, fieldPath, indent, parentName, nestedStructTypes, commonStructs)
		}
	}
}

func generateArrayWrite(buf *strings.Builder, field Field, fieldPath string, elemType string, indent int, parentName string, nestedStructTypes map[string]bool, commonStructs []Field) {
	indentStr := strings.Repeat("\t", indent)

	// Check if nullable
	isNullable := field.NullableVersions != ""

	// Write length
	if isNullable {
		buf.WriteString(fmt.Sprintf("%sif %s == nil {\n", indentStr, fieldPath))
		buf.WriteString(fmt.Sprintf("%s\tif isFlexible {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t\tif err := protocol.WriteVaruint32(w, 0); err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t\t\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t\t}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t} else {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t\tif err := protocol.WriteInt32(w, -1); err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t\t\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t\t}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s} else {\n", indentStr))
	}

	// Write array length
	buf.WriteString(fmt.Sprintf("%s\tif isFlexible {\n", indentStr))
	buf.WriteString(fmt.Sprintf("%s\t\tlength := uint32(len(%s) + 1)\n", indentStr, fieldPath))
	buf.WriteString(fmt.Sprintf("%s\t\tif err := protocol.WriteVaruint32(w, length); err != nil {\n", indentStr))
	buf.WriteString(fmt.Sprintf("%s\t\t\treturn err\n", indentStr))
	buf.WriteString(fmt.Sprintf("%s\t\t}\n", indentStr))
	buf.WriteString(fmt.Sprintf("%s\t} else {\n", indentStr))
	buf.WriteString(fmt.Sprintf("%s\t\tif err := protocol.WriteInt32(w, int32(len(%s))); err != nil {\n", indentStr, fieldPath))
	buf.WriteString(fmt.Sprintf("%s\t\t\treturn err\n", indentStr))
	buf.WriteString(fmt.Sprintf("%s\t\t}\n", indentStr))
	buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))

	// Write elements
	// Check if we need the loop index by checking if the element write will generate code
	needsIndex := needsLoopIndex(elemType, field, commonStructs)
	buf.WriteString(fmt.Sprintf("%sfor i := range %s {\n", indentStr, fieldPath))
	generateArrayElementWrite(buf, field, fieldPath+"[i]", elemType, indent+1, parentName, nestedStructTypes, commonStructs)
	// If the index isn't needed, mark it as intentionally unused
	if !needsIndex {
		buf.WriteString(fmt.Sprintf("%s\t_ = i\n", indentStr))
	}
	buf.WriteString(fmt.Sprintf("%s}\n", indentStr))

	if isNullable {
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	}
}

func generateStructWrite(buf *strings.Builder, field Field, fieldPath string, indent int, parentName string, nestedStructTypes map[string]bool, commonStructs []Field) {
	// Recursively write struct fields
	if len(field.Fields) > 0 {
		generateWriteFields(buf, field.Fields, indent, []int{}, []int{}, fieldPath, parentName, nestedStructTypes, commonStructs)
	}
}

func generateReadFields(buf *strings.Builder, fields []Field, indent int, validVersions []int, flexibleVersions []int, prefix string, parentName string, nestedStructTypes map[string]bool, commonStructs []Field) {
	indentStr := strings.Repeat("\t", indent)

	for _, field := range fields {
		fieldVersions := parseVersionRange(field.Versions)
		if len(fieldVersions) == 0 {
			continue
		}

		fieldName := toExportedName(field.Name)
		fieldPath := prefix + "." + fieldName

		// Check if field should be read for this version
		buf.WriteString(fmt.Sprintf("%s// %s\n", indentStr, field.Name))
		maxVersion := fieldVersions[len(fieldVersions)-1]
		if maxVersion == 999 {
			maxVersion = 999
		}
		buf.WriteString(fmt.Sprintf("%sif version >= %d && version <= %d {\n",
			indentStr, fieldVersions[0], maxVersion))

		// Generate read code based on type
		generateFieldRead(buf, field, fieldPath, indent+1, parentName, nestedStructTypes, commonStructs)

		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	}
}

func generateFieldRead(buf *strings.Builder, field Field, fieldPath string, indent int, parentName string, nestedStructTypes map[string]bool, commonStructs []Field) {
	indentStr := strings.Repeat("\t", indent)

	// Handle tagged fields separately
	if field.Tag != nil {
		return // Tagged fields handled separately
	}

	switch {
	case strings.HasPrefix(field.Type, "[]"):
		// Array type - get the resolved Go type for the element
		elemType := strings.TrimPrefix(field.Type, "[]")
		generateArrayRead(buf, field, fieldPath, elemType, indent, parentName, nestedStructTypes, commonStructs)
	case field.Type == "bool":
		buf.WriteString(fmt.Sprintf("%sval, err := protocol.ReadBool(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s%s = val\n", indentStr, fieldPath))
	case field.Type == "int8":
		buf.WriteString(fmt.Sprintf("%sval, err := protocol.ReadInt8(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s%s = val\n", indentStr, fieldPath))
	case field.Type == "int16":
		buf.WriteString(fmt.Sprintf("%sval, err := protocol.ReadInt16(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s%s = val\n", indentStr, fieldPath))
	case field.Type == "int32":
		buf.WriteString(fmt.Sprintf("%sval, err := protocol.ReadInt32(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s%s = val\n", indentStr, fieldPath))
	case field.Type == "int64":
		buf.WriteString(fmt.Sprintf("%sval, err := protocol.ReadInt64(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s%s = val\n", indentStr, fieldPath))
	case field.Type == "string":
		buf.WriteString(fmt.Sprintf("%sif isFlexible {\n", indentStr))
		if field.NullableVersions != "" {
			buf.WriteString(fmt.Sprintf("%s\tval, err := protocol.ReadCompactNullableString(r)\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\tif err != nil {\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\t\treturn err\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\t%s = val\n", indentStr, fieldPath))
		} else {
			buf.WriteString(fmt.Sprintf("%s\tval, err := protocol.ReadCompactString(r)\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\tif err != nil {\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\t\treturn err\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\t%s = val\n", indentStr, fieldPath))
		}
		buf.WriteString(fmt.Sprintf("%s} else {\n", indentStr))
		if field.NullableVersions != "" {
			buf.WriteString(fmt.Sprintf("%s\tval, err := protocol.ReadNullableString(r)\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\tif err != nil {\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\t\treturn err\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\t%s = val\n", indentStr, fieldPath))
		} else {
			buf.WriteString(fmt.Sprintf("%s\tval, err := protocol.ReadString(r)\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\tif err != nil {\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\t\treturn err\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\t%s = val\n", indentStr, fieldPath))
		}
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	case field.Type == "uuid":
		buf.WriteString(fmt.Sprintf("%sval, err := protocol.ReadUUID(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s%s = val\n", indentStr, fieldPath))
	case field.Type == "bytes" || field.Type == "records":
		// records type is always nullable (*[]byte), bytes can be nullable based on nullableVersions
		isNullable := field.Type == "records" || field.NullableVersions != ""
		buf.WriteString(fmt.Sprintf("%sif isFlexible {\n", indentStr))
		if isNullable {
			buf.WriteString(fmt.Sprintf("%s\tval, err := protocol.ReadCompactNullableBytes(r)\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\tif err != nil {\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\t\treturn err\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\t%s = val\n", indentStr, fieldPath))
		} else {
			buf.WriteString(fmt.Sprintf("%s\tval, err := protocol.ReadCompactBytes(r)\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\tif err != nil {\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\t\treturn err\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\t%s = val\n", indentStr, fieldPath))
		}
		buf.WriteString(fmt.Sprintf("%s} else {\n", indentStr))
		if isNullable {
			buf.WriteString(fmt.Sprintf("%s\tval, err := protocol.ReadNullableBytes(r)\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\tif err != nil {\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\t\treturn err\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\t%s = val\n", indentStr, fieldPath))
		} else {
			buf.WriteString(fmt.Sprintf("%s\tval, err := protocol.ReadBytes(r)\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\tif err != nil {\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\t\treturn err\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
			buf.WriteString(fmt.Sprintf("%s\t%s = val\n", indentStr, fieldPath))
		}
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	default:
		// Struct type - recursive read
		if len(field.Fields) > 0 {
			generateStructRead(buf, field, fieldPath, indent, parentName, nestedStructTypes, commonStructs)
		}
	}
}

func generateArrayRead(buf *strings.Builder, field Field, fieldPath string, elemType string, indent int, parentName string, nestedStructTypes map[string]bool, commonStructs []Field) {
	indentStr := strings.Repeat("\t", indent)

	// Check if nullable
	isNullable := field.NullableVersions != ""

	// Read length
	buf.WriteString(fmt.Sprintf("%svar length int32\n", indentStr))
	buf.WriteString(fmt.Sprintf("%sif isFlexible {\n", indentStr))
	buf.WriteString(fmt.Sprintf("%s\tvar lengthUint uint32\n", indentStr))
	buf.WriteString(fmt.Sprintf("%s\tlengthUint, err := protocol.ReadVaruint32(r)\n", indentStr))
	buf.WriteString(fmt.Sprintf("%s\tif err != nil {\n", indentStr))
	buf.WriteString(fmt.Sprintf("%s\t\treturn err\n", indentStr))
	buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
	if isNullable {
		buf.WriteString(fmt.Sprintf("%s\tif lengthUint == 0 {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t\t%s = nil\n", indentStr, fieldPath))
		buf.WriteString(fmt.Sprintf("%s\t} else {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t\tif lengthUint < 1 {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t\t\treturn errors.New(\"invalid compact array length\")\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t\t}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t\tlength = int32(lengthUint - 1)\n", indentStr))
		// Resolve element type using getGoType
		goElemType := getGoType(elemType, "", []int{}, []int{}, false, parentName, nestedStructTypes)
		buf.WriteString(fmt.Sprintf("%s\t\t%s = make([]%s, length)\n", indentStr, fieldPath, goElemType))
		buf.WriteString(fmt.Sprintf("%s\t\tfor i := int32(0); i < length; i++ {\n", indentStr))
		generateArrayElementRead(buf, field, fieldPath+"[i]", elemType, indent+2, parentName, nestedStructTypes, commonStructs)
		buf.WriteString(fmt.Sprintf("%s\t\t}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
	} else {
		buf.WriteString(fmt.Sprintf("%s\tif lengthUint < 1 {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t\treturn errors.New(\"invalid compact array length\")\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\tlength = int32(lengthUint - 1)\n", indentStr))
		// Resolve element type using getGoType
		goElemType := getGoType(elemType, "", []int{}, []int{}, false, parentName, nestedStructTypes)
		buf.WriteString(fmt.Sprintf("%s\t%s = make([]%s, length)\n", indentStr, fieldPath, goElemType))
		buf.WriteString(fmt.Sprintf("%s\tfor i := int32(0); i < length; i++ {\n", indentStr))
		generateArrayElementRead(buf, field, fieldPath+"[i]", elemType, indent+1, parentName, nestedStructTypes, commonStructs)
		buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
	}
	buf.WriteString(fmt.Sprintf("%s} else {\n", indentStr))
	buf.WriteString(fmt.Sprintf("%s\tvar err error\n", indentStr))
	buf.WriteString(fmt.Sprintf("%s\tlength, err = protocol.ReadInt32(r)\n", indentStr))
	buf.WriteString(fmt.Sprintf("%s\tif err != nil {\n", indentStr))
	buf.WriteString(fmt.Sprintf("%s\t\treturn err\n", indentStr))
	buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
	if isNullable {
		buf.WriteString(fmt.Sprintf("%s\tif length == -1 {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t\t%s = nil\n", indentStr, fieldPath))
		buf.WriteString(fmt.Sprintf("%s\t} else {\n", indentStr))
		// Resolve element type using getGoType
		goElemType := getGoType(elemType, "", []int{}, []int{}, false, parentName, nestedStructTypes)
		buf.WriteString(fmt.Sprintf("%s\t\t%s = make([]%s, length)\n", indentStr, fieldPath, goElemType))
		buf.WriteString(fmt.Sprintf("%s\t\tfor i := int32(0); i < length; i++ {\n", indentStr))
		generateArrayElementRead(buf, field, fieldPath+"[i]", elemType, indent+2, parentName, nestedStructTypes, commonStructs)
		buf.WriteString(fmt.Sprintf("%s\t\t}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
	} else {
		// Resolve element type using getGoType
		goElemType := getGoType(elemType, "", []int{}, []int{}, false, parentName, nestedStructTypes)
		buf.WriteString(fmt.Sprintf("%s\t%s = make([]%s, length)\n", indentStr, fieldPath, goElemType))
		buf.WriteString(fmt.Sprintf("%s\tfor i := int32(0); i < length; i++ {\n", indentStr))
		generateArrayElementRead(buf, field, fieldPath+"[i]", elemType, indent+1, parentName, nestedStructTypes, commonStructs)
		buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
	}
	buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
}

func generateStructRead(buf *strings.Builder, field Field, fieldPath string, indent int, parentName string, nestedStructTypes map[string]bool, commonStructs []Field) {
	// Recursively read struct fields
	if len(field.Fields) > 0 {
		generateReadFields(buf, field.Fields, indent, []int{}, []int{}, fieldPath, parentName, nestedStructTypes, commonStructs)
	}
}

func generateArrayElementWrite(buf *strings.Builder, field Field, elemPath string, elemType string, indent int, parentName string, nestedStructTypes map[string]bool, commonStructs []Field) {
	indentStr := strings.Repeat("\t", indent)

	// Generate write code for the element
	switch {
	case elemType == "bool":
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteBool(w, %s); err != nil {\n", indentStr, elemPath))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	case elemType == "int8":
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteInt8(w, %s); err != nil {\n", indentStr, elemPath))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	case elemType == "int16":
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteInt16(w, %s); err != nil {\n", indentStr, elemPath))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	case elemType == "int32":
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteInt32(w, %s); err != nil {\n", indentStr, elemPath))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	case elemType == "int64":
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteInt64(w, %s); err != nil {\n", indentStr, elemPath))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	case elemType == "string":
		buf.WriteString(fmt.Sprintf("%sif isFlexible {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\tif err := protocol.WriteCompactString(w, %s); err != nil {\n", indentStr, elemPath))
		buf.WriteString(fmt.Sprintf("%s\t\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s} else {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\tif err := protocol.WriteString(w, %s); err != nil {\n", indentStr, elemPath))
		buf.WriteString(fmt.Sprintf("%s\t\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	case elemType == "uuid":
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteUUID(w, %s); err != nil {\n", indentStr, elemPath))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	default:
		// Struct type - need to find the field definition
		if len(field.Fields) > 0 {
			// Recursively write struct
			generateWriteFields(buf, field.Fields, indent, []int{}, []int{}, elemPath, parentName, nestedStructTypes, commonStructs)
		} else {
			// Check if it's a common struct
			commonStruct := findCommonStruct(elemType, commonStructs)
			if commonStruct != nil && len(commonStruct.Fields) > 0 {
				generateWriteFields(buf, commonStruct.Fields, indent, []int{}, []int{}, elemPath, parentName, nestedStructTypes, commonStructs)
			}
		}
	}
}

func generateArrayElementRead(buf *strings.Builder, field Field, elemPath string, elemType string, indent int, parentName string, nestedStructTypes map[string]bool, commonStructs []Field) {
	indentStr := strings.Repeat("\t", indent)

	switch {
	case elemType == "bool":
		buf.WriteString(fmt.Sprintf("%sval, err := protocol.ReadBool(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s%s = val\n", indentStr, elemPath))
	case elemType == "int8":
		buf.WriteString(fmt.Sprintf("%sval, err := protocol.ReadInt8(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s%s = val\n", indentStr, elemPath))
	case elemType == "int16":
		buf.WriteString(fmt.Sprintf("%sval, err := protocol.ReadInt16(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s%s = val\n", indentStr, elemPath))
	case elemType == "int32":
		buf.WriteString(fmt.Sprintf("%sval, err := protocol.ReadInt32(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s%s = val\n", indentStr, elemPath))
	case elemType == "int64":
		buf.WriteString(fmt.Sprintf("%sval, err := protocol.ReadInt64(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s%s = val\n", indentStr, elemPath))
	case elemType == "string":
		buf.WriteString(fmt.Sprintf("%sif isFlexible {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\tval, err := protocol.ReadCompactString(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\tif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t%s = val\n", indentStr, elemPath))
		buf.WriteString(fmt.Sprintf("%s} else {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\tval, err := protocol.ReadString(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\tif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t%s = val\n", indentStr, elemPath))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	case elemType == "uuid":
		buf.WriteString(fmt.Sprintf("%sval, err := protocol.ReadUUID(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s%s = val\n", indentStr, elemPath))
	default:
		// Struct type - need to find the field definition
		if len(field.Fields) > 0 {
			// Recursively read struct
			generateReadFields(buf, field.Fields, indent, []int{}, []int{}, elemPath, parentName, nestedStructTypes, commonStructs)
		} else {
			// Check if it's a common struct
			commonStruct := findCommonStruct(elemType, commonStructs)
			if commonStruct != nil && len(commonStruct.Fields) > 0 {
				generateReadFields(buf, commonStruct.Fields, indent, []int{}, []int{}, elemPath, parentName, nestedStructTypes, commonStructs)
			}
		}
	}
}

func getGoTypeForArray(elemType string, isNullable bool, isFlexible bool, isTagged bool) string {
	switch elemType {
	case "bool":
		return "bool"
	case "int8":
		return "int8"
	case "int16":
		return "int16"
	case "int32":
		return "int32"
	case "int64":
		return "int64"
	case "uint16":
		return "uint16"
	case "uint32":
		return "uint32"
	case "float64":
		return "float64"
	case "string":
		return "string"
	case "uuid":
		return "uuid.UUID"
	case "bytes", "records":
		return "[]byte"
	default:
		// Struct type
		return toExportedName(elemType)
	}
}

func generateTaggedFieldsMethods(buf *strings.Builder, def *MessageDef, validVersions []int, flexibleVersions []int, nestedStructTypes map[string]bool, commonStructs []Field) {
	// Collect all tagged fields
	taggedFields := collectTaggedFields(def.Fields, validVersions, flexibleVersions)

	// Check if any tagged field uses isFlexible (string types in tagged fields use compact encoding)
	needsIsFlexible := false
	for _, tf := range taggedFields {
		if tf.Type == "string" || strings.HasPrefix(tf.Type, "[]") {
			needsIsFlexible = true
			break
		}
		// Check nested fields
		if len(tf.Fields) > 0 {
			if hasStringType(tf.Fields) {
				needsIsFlexible = true
				break
			}
		}
	}

	// Generate writeTaggedFields method
	buf.WriteString(fmt.Sprintf("// writeTaggedFields writes tagged fields for %s.\n", def.Name))
	buf.WriteString(fmt.Sprintf("func (m *%s) writeTaggedFields(w io.Writer, version int16) error {\n", def.Name))
	buf.WriteString("\tvar taggedFieldsCount int\n")
	buf.WriteString("\tvar taggedFieldsBuf bytes.Buffer\n\n")
	// Calculate isFlexible for tagged fields only if needed
	if needsIsFlexible && len(flexibleVersions) > 0 {
		buf.WriteString(fmt.Sprintf("\tisFlexible := version >= %d\n\n", flexibleVersions[0]))
	}

	// Always generate the method, even if there are no tagged fields
	// (it will just write/read 0 tagged fields)
	if len(taggedFields) > 0 {
		for _, tf := range taggedFields {
			fieldVersions := parseVersionRange(tf.Versions)
			taggedVersions := parseVersionRange(tf.TaggedVersions)
			if len(taggedVersions) == 0 {
				taggedVersions = fieldVersions
			}

			fieldName := toExportedName(tf.Name)
			fieldPath := "m." + fieldName

			buf.WriteString(fmt.Sprintf("\t// %s (tag %d)\n", tf.Name, *tf.Tag))
			buf.WriteString(fmt.Sprintf("\tif version >= %d {\n", taggedVersions[0]))

			// Check if field has non-default value or is not ignorable
			// For tagged fields, we write them if they're not the default value
			buf.WriteString("\t\tif ")
			writeNonDefaultCheck(buf, tf, "m."+fieldName)
			buf.WriteString(" {\n")

			// Write tag
			buf.WriteString(fmt.Sprintf("\t\t\tif err := protocol.WriteVaruint32(&taggedFieldsBuf, uint32(%d)); err != nil {\n", *tf.Tag))
			buf.WriteString("\t\t\t\treturn err\n")
			buf.WriteString("\t\t\t}\n")

			// Write field value
			generateTaggedFieldWrite(buf, tf, fieldPath, 2, def.Name, nestedStructTypes, commonStructs)

			buf.WriteString("\t\t\ttaggedFieldsCount++\n")
			buf.WriteString("\t\t}\n")
			buf.WriteString("\t}\n\n")
		}
	}

	buf.WriteString("\t// Write tagged fields count\n")
	buf.WriteString("\tif err := protocol.WriteVaruint32(w, uint32(taggedFieldsCount)); err != nil {\n")
	buf.WriteString("\t\treturn err\n")
	buf.WriteString("\t}\n\n")
	buf.WriteString("\t// Write tagged fields data\n")
	buf.WriteString("\tif taggedFieldsCount > 0 {\n")
	buf.WriteString("\t\tif _, err := w.Write(taggedFieldsBuf.Bytes()); err != nil {\n")
	buf.WriteString("\t\t\treturn err\n")
	buf.WriteString("\t\t}\n")
	buf.WriteString("\t}\n\n")
	buf.WriteString("\treturn nil\n")
	buf.WriteString("}\n\n")

	// Generate readTaggedFields method
	buf.WriteString(fmt.Sprintf("// readTaggedFields reads tagged fields for %s.\n", def.Name))
	buf.WriteString(fmt.Sprintf("func (m *%s) readTaggedFields(r io.Reader, version int16) error {\n", def.Name))
	// Calculate isFlexible for tagged fields only if needed
	if needsIsFlexible && len(flexibleVersions) > 0 {
		buf.WriteString(fmt.Sprintf("\tisFlexible := version >= %d\n\n", flexibleVersions[0]))
	}
	buf.WriteString("\t// Read tagged fields count\n")
	buf.WriteString("\tcount, err := protocol.ReadVaruint32(r)\n")
	buf.WriteString("\tif err != nil {\n")
	buf.WriteString("\t\treturn err\n")
	buf.WriteString("\t}\n\n")
	buf.WriteString("\tif count == 0 {\n")
	buf.WriteString("\t\treturn nil\n")
	buf.WriteString("\t}\n\n")
	buf.WriteString("\t// Read tagged fields\n")
	buf.WriteString("\tfor i := uint32(0); i < count; i++ {\n")
	buf.WriteString("\t\ttag, err := protocol.ReadVaruint32(r)\n")
	buf.WriteString("\t\tif err != nil {\n")
	buf.WriteString("\t\t\treturn err\n")
	buf.WriteString("\t\t}\n\n")
	buf.WriteString("\t\tswitch tag {\n")

	if len(taggedFields) > 0 {
		for _, tf := range taggedFields {
			fieldName := toExportedName(tf.Name)
			fieldPath := "m." + fieldName
			taggedVersions := parseVersionRange(tf.TaggedVersions)
			if len(taggedVersions) == 0 {
				taggedVersions = parseVersionRange(tf.Versions)
			}

			buf.WriteString(fmt.Sprintf("\t\tcase %d: // %s\n", *tf.Tag, tf.Name))
			buf.WriteString(fmt.Sprintf("\t\t\tif version >= %d {\n", taggedVersions[0]))
			// For tagged fields, use the message name as parent
			generateTaggedFieldRead(buf, tf, fieldPath, 3, def.Name, nestedStructTypes, commonStructs)
			buf.WriteString("\t\t\t}\n")
		}
	}

	buf.WriteString("\t\tdefault:\n")
	buf.WriteString("\t\t\t// Unknown tag, skip it\n")
	buf.WriteString("\t\t\t// Read and discard the field data\n")
	buf.WriteString("\t\t\t// For now, we'll need to know the type to skip properly\n")
	buf.WriteString("\t\t\t// This is a simplified implementation\n")
	buf.WriteString("\t\t}\n")
	buf.WriteString("\t}\n\n")
	buf.WriteString("\treturn nil\n")
	buf.WriteString("}\n\n")
}

func collectTaggedFields(fields []Field, validVersions []int, flexibleVersions []int) []Field {
	var tagged []Field
	for _, field := range fields {
		// Only collect tagged fields at the top level, not from nested structs
		// Tagged fields in nested structs are handled within their own context
		if field.Tag != nil {
			tagged = append(tagged, field)
		}
		// Don't recursively collect tagged fields from nested structs
		// They should be handled separately in their own writeTaggedFields methods
	}
	return tagged
}

func writeTaggedFieldCondition(buf *strings.Builder, field Field, fieldName string) {
	// For tagged fields, we need to check if the value is different from default
	// If ignorable is true, we can skip fields with default values
	fieldPath := "m." + fieldName

	if field.Ignorable != nil {
		// If ignorable, only write if not default
		buf.WriteString("\t\t")
		writeNonDefaultCheck(buf, field, fieldPath)
		buf.WriteString(" {\n")
	} else {
		// Always write if not ignorable
		buf.WriteString("\t\tif true {\n")
	}
}

func writeNonDefaultCheck(buf *strings.Builder, field Field, fieldPath string) {
	switch {
	case strings.HasPrefix(field.Type, "[]"):
		// For arrays, check if not nil and not empty (or check against default)
		buf.WriteString(fmt.Sprintf("%s != nil && len(%s) > 0", fieldPath, fieldPath))
	case field.Type == "bool":
		if field.Default != nil {
			if v, ok := field.Default.(string); ok && v == "false" {
				buf.WriteString(fmt.Sprintf("%s", fieldPath))
			} else {
				buf.WriteString(fmt.Sprintf("%s != false", fieldPath))
			}
		} else {
			buf.WriteString(fmt.Sprintf("%s", fieldPath))
		}
	case field.Type == "string":
		// Check if nullable (nullableVersions is set)
		isNullable := field.NullableVersions != ""
		if isNullable {
			// For nullable strings, check if not nil and not empty
			if field.Default != nil {
				if v, ok := field.Default.(string); ok && (v == "" || v == "null") {
					buf.WriteString(fmt.Sprintf("%s != nil && *%s != \"\"", fieldPath, fieldPath))
				} else {
					buf.WriteString(fmt.Sprintf("%s != nil && *%s != %q", fieldPath, fieldPath, v))
				}
			} else {
				buf.WriteString(fmt.Sprintf("%s != nil && *%s != \"\"", fieldPath, fieldPath))
			}
		} else {
			if field.Default != nil {
				if v, ok := field.Default.(string); ok && (v == "" || v == "null") {
					buf.WriteString(fmt.Sprintf("%s != \"\"", fieldPath))
				} else {
					buf.WriteString(fmt.Sprintf("%s != %q", fieldPath, v))
				}
			} else {
				buf.WriteString(fmt.Sprintf("%s != \"\"", fieldPath))
			}
		}
	case field.Type == "bytes" || field.Type == "records":
		buf.WriteString(fmt.Sprintf("%s != nil", fieldPath))
	case field.Type == "int8", field.Type == "int16", field.Type == "int32", field.Type == "int64":
		if field.Default != nil {
			if v, ok := field.Default.(string); ok {
				buf.WriteString(fmt.Sprintf("%s != %s", fieldPath, v))
			} else if v, ok := field.Default.(float64); ok {
				buf.WriteString(fmt.Sprintf("%s != %.0f", fieldPath, v))
			} else {
				buf.WriteString(fmt.Sprintf("%s != 0", fieldPath))
			}
		} else {
			buf.WriteString(fmt.Sprintf("%s != 0", fieldPath))
		}
	default:
		// For structs, always write (or check if not nil)
		buf.WriteString("true")
	}
}

func generateTaggedFieldWrite(buf *strings.Builder, field Field, fieldPath string, indent int, parentName string, nestedStructTypes map[string]bool, commonStructs []Field) {
	indentStr := strings.Repeat("\t", indent)

	switch {
	case field.Type == "bool":
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteBool(&taggedFieldsBuf, %s); err != nil {\n", indentStr, fieldPath))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	case field.Type == "int8":
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteInt8(&taggedFieldsBuf, %s); err != nil {\n", indentStr, fieldPath))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	case field.Type == "int16":
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteInt16(&taggedFieldsBuf, %s); err != nil {\n", indentStr, fieldPath))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	case field.Type == "int32":
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteInt32(&taggedFieldsBuf, %s); err != nil {\n", indentStr, fieldPath))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	case field.Type == "int64":
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteInt64(&taggedFieldsBuf, %s); err != nil {\n", indentStr, fieldPath))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	case field.Type == "string":
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteCompactNullableString(&taggedFieldsBuf, %s); err != nil {\n", indentStr, fieldPath))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	case field.Type == "bytes" || field.Type == "records":
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteCompactNullableBytes(&taggedFieldsBuf, %s); err != nil {\n", indentStr, fieldPath))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	case strings.HasPrefix(field.Type, "[]"):
		// Array in tagged field - use compact array
		buf.WriteString(fmt.Sprintf("%s// Array in tagged field\n", indentStr))
		buf.WriteString(fmt.Sprintf("%slength := uint32(len(%s) + 1)\n", indentStr, fieldPath))
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteVaruint32(&taggedFieldsBuf, length); err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sfor i := range %s {\n", indentStr, fieldPath))
		elemType := strings.TrimPrefix(field.Type, "[]")
		generateTaggedArrayElementWrite(buf, field, fieldPath+"[i]", elemType, indent+1, parentName, nestedStructTypes, commonStructs)
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	default:
		// Struct type
		if len(field.Fields) > 0 {
			generateWriteFields(buf, field.Fields, indent, []int{}, []int{}, fieldPath, parentName, nestedStructTypes, commonStructs)
		} else {
			// Check if it's a common struct
			commonStruct := findCommonStruct(field.Type, commonStructs)
			if commonStruct != nil && len(commonStruct.Fields) > 0 {
				generateWriteFields(buf, commonStruct.Fields, indent, []int{}, []int{}, fieldPath, parentName, nestedStructTypes, commonStructs)
			}
		}
	}
}

func generateTaggedArrayElementWrite(buf *strings.Builder, field Field, elemPath string, elemType string, indent int, parentName string, nestedStructTypes map[string]bool, commonStructs []Field) {
	indentStr := strings.Repeat("\t", indent)

	switch elemType {
	case "bool":
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteBool(&taggedFieldsBuf, %s); err != nil {\n", indentStr, elemPath))
	case "int8":
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteInt8(&taggedFieldsBuf, %s); err != nil {\n", indentStr, elemPath))
	case "int16":
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteInt16(&taggedFieldsBuf, %s); err != nil {\n", indentStr, elemPath))
	case "int32":
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteInt32(&taggedFieldsBuf, %s); err != nil {\n", indentStr, elemPath))
	case "int64":
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteInt64(&taggedFieldsBuf, %s); err != nil {\n", indentStr, elemPath))
	case "string":
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteCompactString(&taggedFieldsBuf, %s); err != nil {\n", indentStr, elemPath))
	case "uuid":
		buf.WriteString(fmt.Sprintf("%sif err := protocol.WriteUUID(&taggedFieldsBuf, %s); err != nil {\n", indentStr, elemPath))
	default:
		// Struct element - recursively write struct fields
		if len(field.Fields) > 0 {
			generateWriteFields(buf, field.Fields, indent, []int{}, []int{}, elemPath, parentName, nestedStructTypes, commonStructs)
		} else {
			// Check if it's a common struct
			commonStruct := findCommonStruct(elemType, commonStructs)
			if commonStruct != nil && len(commonStruct.Fields) > 0 {
				generateWriteFields(buf, commonStruct.Fields, indent, []int{}, []int{}, elemPath, parentName, nestedStructTypes, commonStructs)
			} else {
				buf.WriteString(fmt.Sprintf("%s// TODO: Unknown struct element type\n", indentStr))
				return
			}
		}
		return
	}
	buf.WriteString(fmt.Sprintf("%s\t\treturn err\n", indentStr))
	buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
}

func generateTaggedFieldRead(buf *strings.Builder, field Field, fieldPath string, indent int, parentName string, nestedStructTypes map[string]bool, commonStructs []Field) {
	indentStr := strings.Repeat("\t", indent)

	switch {
	case field.Type == "bool":
		buf.WriteString(fmt.Sprintf("%sval, err := protocol.ReadBool(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s%s = val\n", indentStr, fieldPath))
	case field.Type == "int8":
		buf.WriteString(fmt.Sprintf("%sval, err := protocol.ReadInt8(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s%s = val\n", indentStr, fieldPath))
	case field.Type == "int16":
		buf.WriteString(fmt.Sprintf("%sval, err := protocol.ReadInt16(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s%s = val\n", indentStr, fieldPath))
	case field.Type == "int32":
		buf.WriteString(fmt.Sprintf("%sval, err := protocol.ReadInt32(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s%s = val\n", indentStr, fieldPath))
	case field.Type == "int64":
		buf.WriteString(fmt.Sprintf("%sval, err := protocol.ReadInt64(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s%s = val\n", indentStr, fieldPath))
	case field.Type == "string":
		buf.WriteString(fmt.Sprintf("%sval, err := protocol.ReadCompactNullableString(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s%s = val\n", indentStr, fieldPath))
	case field.Type == "bytes" || field.Type == "records":
		buf.WriteString(fmt.Sprintf("%sval, err := protocol.ReadCompactNullableBytes(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s%s = val\n", indentStr, fieldPath))
	case strings.HasPrefix(field.Type, "[]"):
		// Array in tagged field
		buf.WriteString(fmt.Sprintf("%s// Array in tagged field\n", indentStr))
		buf.WriteString(fmt.Sprintf("%slength, err := protocol.ReadVaruint32(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif length == 0 {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t%s = nil\n", indentStr, fieldPath))
		buf.WriteString(fmt.Sprintf("%s} else {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\tif length < 1 {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t\treturn errors.New(\"invalid compact array length\")\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
		elemType := strings.TrimPrefix(field.Type, "[]")
		// Resolve element type using getGoType
		goElemType := getGoType(elemType, "", []int{}, []int{}, false, parentName, nestedStructTypes)
		buf.WriteString(fmt.Sprintf("%s\t%s = make([]%s, length-1)\n", indentStr, fieldPath, goElemType))
		buf.WriteString(fmt.Sprintf("%s\tfor i := uint32(0); i < length-1; i++ {\n", indentStr))
		generateTaggedArrayElementRead(buf, field, fieldPath+"[i]", elemType, indent+1, parentName, nestedStructTypes, commonStructs)
		buf.WriteString(fmt.Sprintf("%s\t}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
	default:
		// Struct type
		if len(field.Fields) > 0 {
			generateReadFields(buf, field.Fields, indent, []int{}, []int{}, fieldPath, parentName, nestedStructTypes, commonStructs)
		} else {
			// Check if it's a common struct
			commonStruct := findCommonStruct(field.Type, commonStructs)
			if commonStruct != nil && len(commonStruct.Fields) > 0 {
				generateReadFields(buf, commonStruct.Fields, indent, []int{}, []int{}, fieldPath, parentName, nestedStructTypes, commonStructs)
			}
		}
	}
}

func generateTaggedArrayElementRead(buf *strings.Builder, field Field, elemPath string, elemType string, indent int, parentName string, nestedStructTypes map[string]bool, commonStructs []Field) {
	indentStr := strings.Repeat("\t", indent)

	switch elemType {
	case "bool":
		buf.WriteString(fmt.Sprintf("%sval, err := protocol.ReadBool(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s%s = val\n", indentStr, elemPath))
	case "int8":
		buf.WriteString(fmt.Sprintf("%sval, err := protocol.ReadInt8(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s%s = val\n", indentStr, elemPath))
	case "int16":
		buf.WriteString(fmt.Sprintf("%sval, err := protocol.ReadInt16(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s%s = val\n", indentStr, elemPath))
	case "int32":
		buf.WriteString(fmt.Sprintf("%sval, err := protocol.ReadInt32(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s%s = val\n", indentStr, elemPath))
	case "int64":
		buf.WriteString(fmt.Sprintf("%sval, err := protocol.ReadInt64(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s%s = val\n", indentStr, elemPath))
	case "string":
		buf.WriteString(fmt.Sprintf("%sval, err := protocol.ReadCompactString(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s%s = val\n", indentStr, elemPath))
	case "uuid":
		buf.WriteString(fmt.Sprintf("%sval, err := protocol.ReadUUID(r)\n", indentStr))
		buf.WriteString(fmt.Sprintf("%sif err != nil {\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s\treturn err\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s}\n", indentStr))
		buf.WriteString(fmt.Sprintf("%s%s = val\n", indentStr, elemPath))
	default:
		// Struct element - recursively read struct fields
		if len(field.Fields) > 0 {
			generateReadFields(buf, field.Fields, indent, []int{}, []int{}, elemPath, parentName, nestedStructTypes, commonStructs)
		} else {
			// Check if it's a common struct
			commonStruct := findCommonStruct(elemType, commonStructs)
			if commonStruct != nil && len(commonStruct.Fields) > 0 {
				generateReadFields(buf, commonStruct.Fields, indent, []int{}, []int{}, elemPath, parentName, nestedStructTypes, commonStructs)
			} else {
				buf.WriteString(fmt.Sprintf("%s// TODO: Unknown struct element type\n", indentStr))
			}
		}
	}
}

