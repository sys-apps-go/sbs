package main

import (
	"fmt"
	"os"
	"strings"
	"path/filepath"
	"reflect"
	"io/ioutil"
	"flag"
)

func main() {
	dir1 := os.Args[1]
	dir2 := os.Args[2]
	hiddenFiles := flag.Bool("hf", false, "Backup hidden files")
	flag.CommandLine.Parse(os.Args[3:])

	extraFiles := []string{}
	missingFiles := []string{}
	differentFiles := []string{}

	walkFunc := func(path string, info os.FileInfo, err error) error {
		if isHidden(path) && !*hiddenFiles {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		relPath, _ := filepath.Rel(dir1, path)
		path2 := filepath.Join(dir2, relPath)

		if _, err := os.Stat(path2); os.IsNotExist(err) {
			missingFiles = append(missingFiles, relPath)
		} else {
			if !info.IsDir() {
				content1, _ := ioutil.ReadFile(path)
				content2, _ := ioutil.ReadFile(path2)
				if !reflect.DeepEqual(content1, content2) {
					differentFiles = append(differentFiles, relPath)
				}
			}
		}
		return nil
	}

	filepath.Walk(dir1, walkFunc)

	filepath.Walk(dir2, func(path string, info os.FileInfo, err error) error {
		relPath, _ := filepath.Rel(dir2, path)
		path1 := filepath.Join(dir1, relPath)

		if _, err := os.Stat(path1); os.IsNotExist(err) {
			extraFiles = append(extraFiles, relPath)
		}
		return nil
	})

	fmt.Println("Extra Files:")
	for _, file := range extraFiles {
		fmt.Println(file)
	}

	fmt.Println("\nMissing Files:")
	for _, file := range missingFiles {
		fmt.Println(file)
	}

	fmt.Println("\nDifferent Files:")
	for _, file := range differentFiles {
		fmt.Println(file)
	}
}

func isHidden(path string) bool {
	name := filepath.Base(path)

	if strings.HasPrefix(name, ".") {
		return true
	}

	return false
}
