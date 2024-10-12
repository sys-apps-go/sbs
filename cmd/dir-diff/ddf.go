package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"flag"
	"strings"
	"path/filepath"
)

func calculateMD5(filePath string) (string, error) {
	fileInfo, err := os.Lstat(filePath)
	if err != nil {
		return "", err
	}

	// Check if it's a symbolic link
	if fileInfo.Mode()&os.ModeSymlink != 0 {
		target, err := os.Readlink(filePath)
		if err != nil {
			return "", err
		}
		return target, nil
	}

	// Calculate MD5 for regular files
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	hashInBytes := hash.Sum(nil)
	md5Hash := hex.EncodeToString(hashInBytes)
	return md5Hash, nil
}

func compareDirectories(dir1, dir2 string, hiddenFiles bool) error {
	fileMap1 := make(map[string]string)
	fileMap2 := make(map[string]string)

	// Walk through dir1 and calculate MD5 or get link target for each file
	err := filepath.Walk(dir1, func(path string, info os.FileInfo, err error) error {
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

		if info.IsDir() {
			fileMap1[relPath] = "dir"
		} else if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			fileMap1[relPath] = target
		} else {
			md5Hash, err := calculateMD5(path)
			if err != nil {
				return err
			}
			fileMap1[relPath] = md5Hash
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Walk through dir2 and calculate MD5 or get link target for each file
	err = filepath.Walk(dir2, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if isHidden(path) && !hiddenFiles {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		relPath, err := filepath.Rel(dir2, path)
		if err != nil {
			return err
		}

		if info.IsDir() {
			fileMap2[relPath] = "dir"
		} else if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			fileMap2[relPath] = target
		} else if !info.IsDir() {
			md5Hash, err := calculateMD5(path)
			if err != nil {
				return err
			}
			fileMap2[relPath] = md5Hash
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Compare file maps
	fmt.Println("Extra files in", dir1, ":")
	for path := range fileMap1 {
		if _, ok := fileMap2[path]; !ok {
			fmt.Println(path)
		}
	}

	fmt.Println("\nMissing files in", dir1, ":")
	for path := range fileMap2 {
		if _, ok := fileMap1[path]; !ok {
			fmt.Println(path)
		} else {
			// Compare link targets or MD5 hashes for modified files
			if fileMap1[path] != fileMap2[path] {
				fmt.Println("Modified file:", path)
			}
		}
	}

	return nil
}

func main() {
	dir1 := os.Args[1]
	dir2 := os.Args[2]
	hiddenFiles := flag.Bool("hf", false, "Scan hidden files")
	flag.CommandLine.Parse(os.Args[3:])

	err := compareDirectories(dir1, dir2, *hiddenFiles)
	if err != nil {
		fmt.Println("Error:", err)
	}
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
