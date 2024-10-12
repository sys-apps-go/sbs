package main

import (
	"fmt"
	"os"
	"flag"
	"strings"
	"path/filepath"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Usage: go run main.go <dir1> <dir2>")
		os.Exit(1)
	}

	dir1 := os.Args[1]
	dir2 := os.Args[2]
	hiddenFiles := flag.Bool("hf", false, "Scan hidden files")
	flag.CommandLine.Parse(os.Args[3:])

	err := compareDirectories(dir1, dir2, *hiddenFiles)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}

func compareDirectories(dir1, dir2 string, hiddenFiles bool) error {
	return filepath.Walk(dir1, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if isHidden(path) && !hiddenFiles {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		relPath, err := filepath.Rel(dir1, path)
		if err != nil {
			return err
		}

		path2 := filepath.Join(dir2, relPath)

		info2, err := os.Lstat(path2)
		if err != nil {
			if os.IsNotExist(err) {
				fmt.Printf("File/directory only exists in %s: %s\n", dir1, relPath)
				return nil
			}
			return err
		}

		if info.Mode() != info2.Mode() {
			fmt.Printf("Permission mismatch for %s:\n", relPath)
			fmt.Printf("  %s: %s\n", dir1, info.Mode())
			fmt.Printf("  %s: %s\n", dir2, info2.Mode())
		}

		return nil
	})
}

func isHidden(path string) bool {
	// Get the base name of the file or directory
	name := filepath.Base(path)

	// Check if it starts with a dot (Unix-style hidden files)
	if strings.HasPrefix(name, ".") {
		return true
	}
	return false
}
