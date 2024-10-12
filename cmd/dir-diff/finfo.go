package main

import (
	"fmt"
	"os"
	"path/filepath"
	"flag"
	"strings"
)

func countDirFileLink(path string, hiddenFiles bool) (int, int, int, int64, error) {
	var numDirs, numFiles, numLinks int
	var totalSize int64

	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if isHidden(path) && !hiddenFiles {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if info.IsDir() {
			numDirs++
		} else if info.Mode().IsRegular() {
			numFiles++
			totalSize += info.Size()
		} else if info.Mode()&os.ModeSymlink != 0 {
			numLinks++
		}
		return nil
	})

	return numDirs, numFiles, numLinks, totalSize, err
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <directory_path>")
		return
	}

	dirPath := os.Args[1]
	hiddenFiles := flag.Bool("hf", false, "Scan hidden files")
	flag.CommandLine.Parse(os.Args[2:])

	numDirs, numFiles, numLinks, totalSize, err := countDirFileLink(dirPath, *hiddenFiles)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Directories: %d\n", numDirs)
	fmt.Printf("Files: %d\n", numFiles)
	fmt.Printf("Links: %d\n", numLinks)
	fmt.Printf("Total Size: %v\n", totalSize/(1024*1024))
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
