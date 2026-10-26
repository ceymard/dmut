package mutations

import (
	"context"
	"embed"
	"encoding/json"
	"io/fs"

	"github.com/ceymard/dmut/v3/mutations/model"
	"github.com/jackc/pgx/v5"
	"github.com/samber/oops"
)

// DmutNamespace is the namespace of dmut's own definition, which creates the tables below.
const DmutNamespace = "__dmut__"

//go:embed dmut-mutations/*.yml
var dmutFiles embed.FS

// dmutFS is dmut's own definition, as a file system rooted on its files.
func dmutFS() fs.FS {
	sub, err := fs.Sub(dmutFiles, "dmut-mutations")
	if err != nil {
		panic(err) // the directory is embedded
	}
	return sub
}

// LoadProject loads the mutation files under paths, preceded by dmut's own definition.
func LoadProject(paths ...string) (*model.Project, error) {
	return model.Load([]fs.FS{dmutFS()}, paths...)
}

// stateQuery reads every mutation of a namespace with its statements and dependencies.
const stateQuery = `
select m.name, m.file, m.revision, m.structural, m.reapply_on,
  coalesce((select jsonb_agg(jsonb_build_object('kind', s.kind, 'up', s.up, 'down', s.down, 'digest', s.digest, 'saved', s.saved) order by s.position)
            from __dmut__.statements s where s.namespace = m.namespace and s.mutation = m.name), '[]'::jsonb),
  coalesce((select jsonb_agg(jsonb_build_object('entry', d.entry, 'reapply_on', d.reapply_on, 'matched', d.matched) order by d.position)
            from __dmut__.deps d where d.namespace = m.namespace and d.mutation = m.name), '[]'::jsonb)
from __dmut__.mutations m
where m.namespace = $1`

type statementJSON struct {
	Kind   model.Kind         `json:"kind"`
	Up     string             `json:"up"`
	Down   *string            `json:"down"`
	Digest string             `json:"digest"`
	Saved  map[string]*string `json:"saved"`
}

type depJSON struct {
	Entry     string   `json:"entry"`
	ReapplyOn []string `json:"reapply_on"`
	Matched   []string `json:"matched"`
}

// LoadState reads what the database records for a namespace, and the highest revision it
// holds (0 when it holds nothing). A database that dmut has not touched has no state; one
// written by an earlier version of dmut is an error.
func (r *Runner) LoadState(ctx context.Context, namespace string) (model.State, int, error) {
	state := model.State{}
	var present, oldFormat bool
	const probe = `select to_regclass('__dmut__.mutations') is not null and to_regclass('__dmut__.statements') is not null,
		exists (select 1 from information_schema.columns where table_schema = '__dmut__' and table_name = 'mutations' and column_name = 'core')`
	if err := r.conn.QueryRow(ctx, probe).Scan(&present, &oldFormat); err != nil {
		return nil, 0, wrapPgError(err, probe)
	}
	if oldFormat {
		return nil, 0, oops.In("pg").Errorf("the schema __dmut__ was written by an earlier version of dmut ; it cannot be read, see the migration guide")
	}
	if !present {
		return state, 0, nil
	}

	rows, err := r.conn.Query(ctx, stateQuery, namespace)
	if err != nil {
		return nil, 0, wrapPgError(err, stateQuery)
	}
	defer rows.Close()
	revision := 0
	for rows.Next() {
		rec := &model.RecordedMutation{}
		var reapplyOn []string
		var statements, deps []byte
		if err := rows.Scan(&rec.Name, &rec.File, &rec.Revision, &rec.Structural, &reapplyOn, &statements, &deps); err != nil {
			return nil, 0, wrapPgError(err, stateQuery)
		}
		rec.ReapplyOn = triggers(reapplyOn)
		var stmts []statementJSON
		if err := json.Unmarshal(statements, &stmts); err != nil {
			return nil, 0, oops.In("pg").With("mutation", rec.Name).Wrapf(err, "can't read the statements")
		}
		for _, s := range stmts {
			st := model.RecordedStatement{Kind: s.Kind, Up: s.Up, Digest: s.Digest, Saved: s.Saved}
			if s.Down != nil {
				st.Down = *s.Down
			}
			rec.Statements = append(rec.Statements, st)
		}
		var depList []depJSON
		if err := json.Unmarshal(deps, &depList); err != nil {
			return nil, 0, oops.In("pg").With("mutation", rec.Name).Wrapf(err, "can't read the dependencies")
		}
		for _, d := range depList {
			rec.Deps = append(rec.Deps, model.RecordedDep{Entry: d.Entry, ReapplyOn: triggers(d.ReapplyOn), Matched: d.Matched})
		}
		revision = max(revision, rec.Revision)
		state[rec.Name] = rec
	}
	if err := rows.Err(); err != nil {
		return nil, 0, wrapPgError(err, stateQuery)
	}
	return state, revision, nil
}

// triggers keeps the difference between no list (nil) and an empty one.
func triggers(list []string) []model.Trigger {
	if list == nil {
		return nil
	}
	out := make([]model.Trigger, len(list))
	for i, s := range list {
		out[i] = model.Trigger(s)
	}
	return out
}

func triggerStrings(list []model.Trigger) []string {
	if list == nil {
		return nil
	}
	out := make([]string, len(list))
	for i, t := range list {
		out[i] = string(t)
	}
	return out
}

// Namespaces lists the namespaces recorded in the database, dmut's own excluded.
func (r *Runner) Namespaces(ctx context.Context) ([]string, error) {
	const sql = `select distinct namespace from __dmut__.mutations where namespace <> $1 order by namespace`
	var present bool
	if err := r.conn.QueryRow(ctx, `select to_regclass('__dmut__.mutations') is not null`).Scan(&present); err != nil {
		return nil, wrapPgError(err, "to_regclass")
	}
	if !present {
		return nil, nil
	}
	rows, err := r.conn.Query(ctx, sql, DmutNamespace)
	if err != nil {
		return nil, wrapPgError(err, sql)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var ns string
		if err := rows.Scan(&ns); err != nil {
			return nil, wrapPgError(err, sql)
		}
		out = append(out, ns)
	}
	return out, wrapPgError(rows.Err(), sql)
}

// DeleteStatementsFrom forgets the statements of a mutation from index from (0-based) on.
func (r *Runner) DeleteStatementsFrom(ctx context.Context, namespace, mutation string, from int) error {
	const sql = `delete from __dmut__.statements where namespace = $1 and mutation = $2 and position > $3`
	return r.Exec(ctx, sql, namespace, mutation, from)
}

// DeleteMutations forgets mutations, with their statements and dependencies.
func (r *Runner) DeleteMutations(ctx context.Context, namespace string, names []string) error {
	if len(names) == 0 {
		return nil
	}
	const sql = `delete from __dmut__.mutations where namespace = $1 and name = any($2)`
	return r.Exec(ctx, sql, namespace, names)
}

// InsertStatements records statements of a mutation, the first one at index from (0-based).
func (r *Runner) InsertStatements(ctx context.Context, namespace, mutation string, from int, stmts []model.RecordedStatement) error {
	const sql = `insert into __dmut__.statements (namespace, mutation, position, kind, up, down, digest, saved)
		values ($1, $2, $3, $4, $5, nullif($6, ''), $7, $8)`
	b := &pgx.Batch{}
	for i, s := range stmts {
		var saved any
		if s.Saved != nil {
			j, err := json.Marshal(s.Saved)
			if err != nil {
				return oops.In("pg").Wrap(err)
			}
			saved = j
		}
		b.Queue(sql, namespace, mutation, from+i+1, string(s.Kind), s.Up, s.Down, s.Digest, saved)
	}
	return r.sendBatch(ctx, b, sql)
}

// SaveMutation records the row of a mutation and replaces its dependencies. Its statements
// are recorded by InsertStatements.
func (r *Runner) SaveMutation(ctx context.Context, namespace string, rec *model.RecordedMutation) error {
	const upsert = `insert into __dmut__.mutations (namespace, name, file, revision, structural, reapply_on)
		values ($1, $2, $3, $4, $5, $6)
		on conflict (namespace, name) do update
		set file = excluded.file, revision = excluded.revision, structural = excluded.structural, reapply_on = excluded.reapply_on, ts = now()`
	const clear = `delete from __dmut__.deps where namespace = $1 and mutation = $2`
	const insert = `insert into __dmut__.deps (namespace, mutation, position, entry, reapply_on, matched) values ($1, $2, $3, $4, $5, $6)`
	b := &pgx.Batch{}
	b.Queue(upsert, namespace, rec.Name, rec.File, rec.Revision, rec.Structural, triggerStrings(rec.ReapplyOn))
	b.Queue(clear, namespace, rec.Name)
	for i, d := range rec.Deps {
		matched := d.Matched
		if matched == nil {
			matched = []string{}
		}
		b.Queue(insert, namespace, rec.Name, i+1, d.Entry, triggerStrings(d.ReapplyOn), matched)
	}
	return r.sendBatch(ctx, b, upsert)
}

// SetRevision marks every mutation of the namespace as being at revision.
func (r *Runner) SetRevision(ctx context.Context, namespace string, revision int) error {
	return r.Exec(ctx, `update __dmut__.mutations set revision = $2 where namespace = $1 and revision <> $2`, namespace, revision)
}

// sendBatch runs the queued queries in one round trip; sql names the batch in errors.
func (r *Runner) sendBatch(ctx context.Context, b *pgx.Batch, sql string) error {
	if b.Len() == 0 {
		return nil
	}
	br := r.conn.SendBatch(ctx, b)
	for range b.Len() {
		if _, err := br.Exec(); err != nil {
			_ = br.Close()
			return wrapPgError(err, sql)
		}
	}
	return wrapPgError(br.Close(), sql)
}
