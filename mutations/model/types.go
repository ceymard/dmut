// Package model defines mutations as written in files: their statements, dependencies and
// reapply triggers, and loads them from yaml.
package model

// Trigger names an event that makes a mutation reapply.
type Trigger string

const (
	DepReapplied Trigger = "dep_reapplied"
	DepAppended  Trigger = "dep_appended"
	DepsGrow     Trigger = "deps_grow"
	DepsShrink   Trigger = "deps_shrink"
)

var allTriggers = []Trigger{DepReapplied, DepAppended, DepsGrow, DepsShrink}

// Kind tells how a statement behaves when its mutation goes up or down.
type Kind string

const (
	// KindReversible has an up and a down.
	KindReversible Kind = "reversible"
	// KindSeed runs on every up, has no down and is skipped on down.
	KindSeed Kind = "seed"
	// KindCheck asserts a condition on every up, has no down and is skipped on down.
	KindCheck Kind = "check"
)

// Mode is how a run reacts to a change that is not an append.
type Mode string

const (
	ModeSafe   Mode = "safe"
	ModeNormal Mode = "normal"
	ModeForce  Mode = "force"
)

// Setting is a named value handed to a statement. Tracked makes a change of its value
// a change of the statement; Saved stores the value so the statement's down receives it.
type Setting struct {
	Name    string
	Tracked bool
	Saved   bool
}

// Statement is one entry of a mutation's sql list.
type Statement struct {
	Kind Kind
	Up   string
	// Down is empty for seeds and checks.
	Down     string
	Settings []Setting
	// Data holds the rows of a seed, each value as text; a nil value is NULL.
	Data [][]*string
	// TextHash and DataHash identify the statement; settings values are not part of them.
	TextHash string
	DataHash string

	explicitDown bool
}

// DepEntry is one item of a mutation's deps: an exact name or a pattern.
type DepEntry struct {
	Pattern string
	// ReapplyOn is nil when the entry does not set it.
	ReapplyOn []Trigger
}

// Mutation is a named list of statements with its dependencies.
type Mutation struct {
	Name       string
	Namespace  string
	File       string
	Structural bool
	Deps       []DepEntry
	// ReapplyOn is nil when the mutation does not set it.
	ReapplyOn  []Trigger
	Statements []Statement

	// Parents are the existing ancestors of the dotted name; set by Revision.Resolve.
	Parents []*Mutation
	// Matched holds, for each entry of Deps, the mutations it matched; set by Revision.Resolve.
	Matched [][]*Mutation
}

// Warning is a problem that does not stop a load.
type Warning struct {
	File     string
	Mutation string
	// Position is the 1-based statement index, or 0 when the warning is not about a statement.
	Position int
	Message  string
}

// Revision is the set of mutations of a namespace at one revision number.
type Revision struct {
	Namespace string
	Number    int
	// Mode is empty when the revision does not set it.
	Mode   Mode
	Delete []string
	// Override holds the entries recorded in place of Mutations; they are never run.
	Override  []*Mutation
	Mutations []*Mutation

	byName map[string]*Mutation
}

// Namespace is an isolated set of mutations, in revisions numbered without gaps.
type Namespace struct {
	Name      string
	Revisions map[int]*Revision
	// Min and Max are the lowest and highest revision numbers.
	Min, Max int
}

// Project is everything loaded from a list of paths.
type Project struct {
	Namespaces []*Namespace
	Warnings   []Warning
}
