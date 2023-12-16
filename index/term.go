package index

import (
	"fmt"
)

type TermIndex struct {
	TableName string
	FieldName string
}

func (i TermIndex) GetIndexKey() string {
	return fmt.Sprintf("term:%s:%s", i.TableName, i.FieldName)
}

func (i TermIndex) MakeValueKey(fieldValue any) string {
	switch value := fieldValue.(type) {
	case int64:
		return fmt.Sprint(value)
	case *int64:
		if value == nil {
			return "null"
		}
		return fmt.Sprint(*value)
	default:
		panic(fmt.Sprintf("Unsupported key type: %T", value))
	}
}

type Term interface {
	int64 | *int64
}
