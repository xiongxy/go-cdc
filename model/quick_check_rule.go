package model

import mapset "github.com/deckarep/golang-set"

type QuickCheckRule struct {
	TableColumn         map[string][]string
	TableExistMap       map[string]bool
	QuickReferenceTable map[string]mapset.Set
}
