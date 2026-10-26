package mutations

import (
	"context"
	"sort"
	"strings"

	"github.com/ceymard/dmut/v2/mutations/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/samber/oops"
)

// up runs the statements of m from index from, each with the settings it lists.
func (a *applier) up(ctx context.Context, ns string, m *model.Mutation, from int) error {
	for i := from; i < len(m.Statements); i++ {
		st := m.Statements[i]
		loc := location{Namespace: ns, Mutation: m.Name, File: m.File, Position: i + 1}
		names := make([]string, len(st.Settings))
		values := make([]*string, len(st.Settings))
		for j, s := range st.Settings {
			names[j], values[j] = s.Name, a.values[s.Name]
		}
		if err := a.r.setSettings(ctx, loc, names, values); err != nil {
			return err
		}
		runs := 1
		if a.seedsTwice && st.Kind == model.KindSeed {
			runs = 2
		}
		for range runs {
			var err error
			switch {
			case st.Kind == model.KindCheck:
				err = a.r.check(ctx, loc, st.Up)
			case st.Data != nil:
				err = a.r.execRows(ctx, loc, st.Up, st.Data)
			default:
				err = a.r.execAt(ctx, loc, st.Up)
			}
			if err != nil {
				return err
			}
		}
		if err := a.r.clearSettings(ctx, loc, names); err != nil {
			return err
		}
	}
	return nil
}

// down runs the downs of the recorded statements of rec from the last one back to index from.
// Seeds and checks have none. A down sees the settings its statement saved, and nothing else.
func (a *applier) down(ctx context.Context, ns string, rec *model.RecordedMutation, from int) error {
	for i := len(rec.Statements) - 1; i >= from; i-- {
		st := rec.Statements[i]
		if st.Kind != model.KindReversible {
			continue
		}
		loc := location{Namespace: ns, Mutation: rec.Name, File: rec.File, Position: i + 1}
		names := make([]string, 0, len(st.Saved))
		for name := range st.Saved {
			names = append(names, name)
		}
		sort.Strings(names)
		values := make([]*string, len(names))
		for j, name := range names {
			values[j] = st.Saved[name]
		}
		if err := a.r.setSettings(ctx, loc, names, values); err != nil {
			return err
		}
		if err := a.r.execAt(ctx, loc, st.Down); err != nil {
			return err
		}
		if err := a.r.clearSettings(ctx, loc, names); err != nil {
			return err
		}
	}
	return nil
}

// execRows runs sql once for each row, in one batch. Values go as text, which Postgres reads
// for the type of the parameter, as COPY does.
func (r *Runner) execRows(ctx context.Context, loc location, sql string, rows [][]*string) error {
	if len(rows) == 0 {
		return nil
	}
	r.logger.Debug("sql", "sql", sql, "rows", len(rows))
	b := &pgx.Batch{}
	for _, row := range rows {
		args := make([]any, len(row))
		for i, v := range row {
			if v != nil {
				args[i] = *v
			}
		}
		b.Queue(sql, args...)
	}
	br := r.conn.SendBatch(ctx, b)
	for range rows {
		if _, err := br.Exec(); err != nil {
			_ = br.Close()
			return locate(loc, wrapPgError(err, sql))
		}
	}
	return locate(loc, wrapPgError(br.Close(), sql))
}

// check runs a query that must return exactly one boolean, and fails the run unless it is true.
func (r *Runner) check(ctx context.Context, loc location, sql string) error {
	r.logger.Debug("sql", "sql", sql)
	rows, err := r.conn.Query(ctx, sql)
	if err != nil {
		return locate(loc, wrapPgError(err, sql))
	}
	defer rows.Close()
	fields := rows.FieldDescriptions()
	if len(fields) != 1 || fields[0].DataTypeOID != pgtype.BoolOID {
		return locate(loc, oops.In("check").Errorf("a check must return one boolean column, got %d column(s) ; %q", len(fields), firstWords(sql)))
	}
	count, value := 0, false
	for rows.Next() {
		count++
		if err := rows.Scan(&value); err != nil {
			return locate(loc, wrapPgError(err, sql))
		}
	}
	if err := rows.Err(); err != nil {
		return locate(loc, wrapPgError(err, sql))
	}
	if count != 1 {
		return locate(loc, oops.In("check").Errorf("a check must return exactly one row, got %d ; %q", count, firstWords(sql)))
	}
	if !value {
		return locate(loc, oops.In("check").Errorf("check failed: %q", firstWords(sql)))
	}
	return nil
}

// locate adds the place of a statement to an error.
func locate(loc location, err error) error {
	if err == nil {
		return nil
	}
	oo := oops.In("pg")
	if loc.Mutation != "" {
		oo = oo.With("namespace", loc.Namespace).With("mutation", loc.Mutation).With("file", loc.File)
		if loc.Position > 0 {
			oo = oo.With("statement_index", loc.Position)
		}
	}
	return oo.Wrap(err)
}

// firstWords names a statement by its first words, for messages.
func firstWords(sql string) string {
	words := strings.Fields(sql)
	if len(words) > 4 {
		words = words[:4]
	}
	return strings.Join(words, " ")
}
