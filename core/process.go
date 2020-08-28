package core

import (
	"github.com/deckarep/golang-set"
	"log"
)

func ProcessAction(set mapset.Set) {
	slice := set.ToSlice()
	for _, v := range slice {
		log.Println("触发操作" + v.(string))
	}
}
