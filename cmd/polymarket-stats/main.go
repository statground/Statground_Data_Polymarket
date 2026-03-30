package main

import (
	"fmt"
	"os"

	"github.com/statground/Statground_Data_Polymarket/internal/polymarket"
)

func main() {
	if err := polymarket.RunStatsReport(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
