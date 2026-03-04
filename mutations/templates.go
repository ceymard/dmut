package mutations

import (
	"bytes"
	"text/template"

	"github.com/Masterminds/sprig/v3"
)

type DmutOptions struct {
	DmutSchema string
}

type TemplateContext struct {
	Options DmutOptions
}

func RunTemplate(templ string, context TemplateContext) (string, error) {
	sp := sprig.FuncMap()

	tpl, err := template.New("").Funcs(sp).Parse(templ)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	if err := tpl.Execute(&buf, map[string]any{
		"Options": context.Options,
	}); err != nil {
		return "", err
	}
	return "", nil

}
