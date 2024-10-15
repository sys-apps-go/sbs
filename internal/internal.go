package internal

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/minio/minio-go/v7"
	miniocredentials "github.com/minio/minio-go/v7/pkg/credentials"
	"golang.org/x/crypto/ssh/terminal"
)

const (
	statusBackup     = "backup"
	statusInit       = "init"
	statusRestore    = "restore"
	statusListBackup = "listbackup"
)

const (
	fileCompressed = "compressed"
	fileEncrypted  = "encrypted"
	fileRegular    = "regular"
)

const (
	backupDirName   = "backup"
	metadataDirName = "metadata"

	backupMeta        = "snapshot.meta"
	snapshotIDFileMap = "snapshot.id"
	backupList        = "backup.list"
)

var (
	keySize       = 16
	encryptionKey = []byte("your-16-byte-key")
	jobQueueSize  = 1
)

type BackupRecord struct {
	RelPath    string      `json:"rel_path"`
	DstPath    string      `json:"dst_path"`
	Type       string      `json:"type"`
	Size       int64       `json:"size"`
	Permission fs.FileMode `json:"permission"`
	ModTime    time.Time   `json:"mod_time"`
	LinkTarget string      `json:"link_target"`
}

type SnapshotRecord struct {
	SnapshotID   string `json:"snapshot_id"`
	MetadataFile string `json:"metadata_file"`
	FileFormat   string `json:"file_format"`
}

type BackupListRecord struct {
	SourceDirName string `json:"source_dir_name"`
	SnapshotID    string `json:"snapshot_id"`
	Version       int    `json:"version"`
	HashCode      string `json:"hash_code"`
	BackupTime    string `json:"backup_time"`
}

type Sbs struct {
	storageType         string
	repoPath            string // /tmp/.repopath
	backupDataDir       string // /tmp/.repopath/backup
	metadataDir         string // /tmp/.repopath/metadata
	snapshotIDFilePath  string // /tmp/.repopath/metadata/snapshot.id
	backupListFilePath  string // /tmp/.repopath/metadata/backup.list
	backupMetaFilePath  string // /tmp/.repopath/backup/<backup version>/backup.meta
	currBackupDstDir    string
	currRestoreDstDir   string
	passwordFilePath    string
	fileCount           int
	dirCount            int
	linkCount           int
	duplicateFilesCount int
	duplicateFilesSize  int64
	backupFileCount     int
	restoreFileCount    int
	totalSize           int64
	fileMu              sync.Mutex
	totalFiles          int64
	completedFiles      int64
	currentStatus       string
	isEncrypted         bool
	isVerboseOutput     bool
	isCompressed        bool
	TaskAborted         uint32
	TaskExited          uint32
	backupRecords       []BackupRecord
	minioClient         *minio.Client
	blockHashMap        map[string]string
	hostName            string
	bucketName          string
	accessKeyID         string
	secretAccessKey     string
	endpoint            string
	minioSrcDir         string
	objectName          string
	s3Client            *s3.S3
	region              string
	basePath            string
}

type job struct {
	srcPath  string
	dstPath  string
	relPath  string
	fileInfo os.FileInfo
}

const (
	TypeFile      = "file"
	TypeDirectory = "directory"
	TypeLink      = "link"
)

type ObjectType string

type ExtendedObjectInfo struct {
	minio.ObjectInfo
	Type ObjectType
}

func NewBackupSystem(storageType string) *Sbs {
	return &Sbs{
		storageType:   storageType,
		blockHashMap:  make(map[string]string),
		backupRecords: make([]BackupRecord, 0),
	}
}

func (s *Sbs) InitCmd(ctx context.Context) error {
	s.currentStatus = statusInit
	repoPath := s.repoPath

	s.initDataDir()

	password, err := s.readPassword("Enter password: ")
	if err != nil {
		return fmt.Errorf("failed to read password: %v", err)
	}
	passwordRepeat, err := s.readPassword("Re-enter password: ")
	if err != nil {
		return fmt.Errorf("failed to read password: %v", err)
	}
	if password != passwordRepeat {
		return fmt.Errorf("Password mismatch")
	}

	encryptedPassword, err := encrypt([]byte(password))
	if err != nil {
		return fmt.Errorf("failed to encrypt password: %v", err)
	}

	passwordFilePath := s.passwordFilePath

	if err := s.writeFileByStorage(passwordFilePath, []byte(encryptedPassword), 0600); err != nil {
		return fmt.Errorf("failed to write encrypted password to file: %v", err)
	}

	fmt.Printf("Repository initialized successfully at %s\n", repoPath)
	return nil
}

func (s *Sbs) InitializeData(repoPathInput string, storageType string, accessKeyID,
	secretAccessKey, endpoint, region string, isVerboseOutput bool) error {
	s.backupRecords = make([]BackupRecord, 0)
	repoPath := repoPathInput
	if repoPath == "" {
		repoPath = filepath.Join(os.Getenv("HOME"), ".repoPath")
	}
	s.storageType = storageType
	s.accessKeyID = accessKeyID
	s.secretAccessKey = secretAccessKey
	s.endpoint = endpoint
	s.isVerboseOutput = isVerboseOutput

	switch storageType {
	case "local", "minio":
		s.repoPath = repoPath
		s.backupDataDir = filepath.Join(repoPath, backupDirName)               //backup dir
		s.metadataDir = filepath.Join(repoPath, metadataDirName)               //metadata dir
		s.backupListFilePath = filepath.Join(s.metadataDir, backupList)        //backup.list
		s.snapshotIDFilePath = filepath.Join(s.metadataDir, snapshotIDFileMap) //snapshot.id
		s.passwordFilePath = filepath.Join(repoPath, "password.enc")
		if storageType == "minio" {
			fl := strings.Split(endpoint, ":")
			s.hostName = fl[0]

			parts := strings.SplitN(repoPath, "/", 3)
			if len(parts) < 3 {
				return fmt.Errorf("Invalid Minio parameters")
			}
			bucketName := parts[1]
			s.bucketName = bucketName

			// Create MinIO client
			minioClient, err := getMinioClient(accessKeyID, secretAccessKey, endpoint)
			if err != nil {
				return err
			}
			s.minioClient = minioClient
			err = s.verifyAndCreateBucket(bucketName, region)
			if err != nil {
				return err
			}
			return nil
		}
	case "aws-s3":
		cleanRepoPath := strings.TrimPrefix(repoPath, "s3://")
		parts := strings.SplitN(cleanRepoPath, "/", 2) // Now split into 2 parts: bucket and rest of the path
		if len(parts) < 2 {
			return fmt.Errorf("Invalid S3 repoPath")
		}
		bucketName := parts[0]
		basePath := parts[1] // ".repopath"

		repoPath := strings.TrimPrefix(cleanRepoPath, bucketName)
		if strings.HasPrefix(repoPath, "/") {
			repoPath = repoPath[1:]
		}
		s.repoPath = repoPath
		s.passwordFilePath = filepath.Join(repoPath, "password.enc")
		s.storageType = storageType

		s.backupDataDir = basePath + "/" + backupDirName               // ".repopath/backup"
		s.metadataDir = basePath + "/" + metadataDirName               // ".repopath/metadata"
		s.backupListFilePath = s.metadataDir + "/" + backupList        // ".repopath/metadata/backup.list"
		s.snapshotIDFilePath = s.metadataDir + "/" + snapshotIDFileMap //".repopath/metadata/snapshot.id"

		s.bucketName = bucketName
		s.region = region
		s.basePath = basePath
		// Create AWS S3 client
		sess, err := session.NewSession(&aws.Config{
			Region:           aws.String(region),   // Replace with appropriate region
			Endpoint:         aws.String(endpoint), // Specify your custom endpoint (MinIO or AWS S3)
			S3ForcePathStyle: aws.Bool(true),       // Enable for path-style access
			Credentials:      credentials.NewStaticCredentials(accessKeyID, secretAccessKey, ""),
		})
		if err != nil {
			return fmt.Errorf("failed to create AWS session: %v", err)
		}

		s.s3Client = s3.New(sess)
	case "remote":
	}
	return nil
}

func (s *Sbs) initDataDir() error {
	// Delete and Recreate Repopath Directory
	fmt.Printf("Deleting directories under %v...\n", s.repoPath)
	if err := s.removeDirByStorage(s.repoPath); err != nil {
		return fmt.Errorf("failed to remove repository directory: %v", err)
	}
	if err := s.createDirByStorage(s.repoPath, 0755); err != nil {
		return fmt.Errorf("failed to create repository directory: %v", err)
	}

	if err := s.createDirByStorage(s.backupDataDir, 0755); err != nil {
		return err
	}
	if err := s.createDirByStorage(s.metadataDir, 0755); err != nil {
		return err
	}

	switch s.storageType {
	case "local", "minio":
		err := s.openFileByStorage(s.backupListFilePath)
		if err != nil {
			return fmt.Errorf("failed to open file: %v", err)
		}

		err = s.openFileByStorage(s.snapshotIDFilePath)
		if err != nil {
			return fmt.Errorf("failed to open file: %v", err)
		}
	case "aws-s3":
		// Skip for now
	case "remote":
	}
	return nil
}

func (s *Sbs) BackupCmd(ctx context.Context, srcDir string, isEncrypted bool, isCompressed bool, backupHiddenFiles bool, numGoroutines int) error {
	var err error
	var sourceDir string
	s.currentStatus = statusBackup

	if err := s.validatePassword(); err != nil {
		return err
	}

	sourceDir = srcDir
	if !filepath.IsAbs(srcDir) {
		absSourceDir, err := filepath.Abs(srcDir)
		if err != nil {
			return fmt.Errorf("failed to get absolute path of source directory: %v", err)
		}
		sourceDir = absSourceDir
	}

	var backupID, snapShotMetaFile, backupMetaFile string
	var version int

	s.isEncrypted = isEncrypted
	s.isCompressed = isCompressed

	runtime.GOMAXPROCS(numGoroutines)

	isFullBackup, err := s.doBackup(sourceDir, &snapShotMetaFile, &backupMetaFile, &version, &backupID, isEncrypted, backupHiddenFiles, numGoroutines)
	if err != nil {
		fmt.Printf("Error doing Backup: %v\n", err)
		return err
	}
	if isFullBackup {
		err := s.saveBackupMetadataJSONFile(backupMetaFile)
		if err != nil {
			fmt.Printf("Error saving Backup Meta: %v\n", err)
			return err
		}
		err = s.appendToSnapShotMetadataFile(snapShotMetaFile, backupMetaFile, backupID)
		if err != nil {
			fmt.Printf("Error saving Snapshot Meta: %v\n", err)
			return err
		}
		s.addRecordInBackupListFile(sourceDir, 0, backupID)
		fmt.Printf("\n\nFull Backup completed successfully, Backup ID: %s\n", backupID)
		fmt.Printf("\nFiles: %v, Directories: %v, Links: %v, Duplicate Files: %v, Total File Size: %v, Size Deduplicated: %v\n",
			s.fileCount, s.dirCount, s.linkCount, s.duplicateFilesCount,
			formatMemoryGB(float64(s.totalSize)), formatMemoryGB(float64(s.duplicateFilesSize)))
	}
	return nil
}

func (s *Sbs) doBackup(srcDir string, snapMetaFile *string, backupMetaFile *string, version *int, snapShotID *string,
	isEncrypted bool, backupHiddenFiles bool, numGoroutines int) (bool, error) {
	var wg sync.WaitGroup
	var backupPath, bkpVersionDir string

	jobChannel := make(chan job, 1024)
	errors := make(chan error, 100)

	// Check if source directory exists
	isDir, _, err := isDirExists(srcDir)
	if err != nil || !isDir {
		return false, fmt.Errorf("Backup path is not a directory: %v %v", srcDir, err)
	}

	// Generate hash for the source directory
	dirNameHashed := generateMD5Hash([]byte(srcDir))
	rootPathDstDir := filepath.Join(s.backupDataDir, dirNameHashed)
	incrementalBackup := false

	var prevBkpMetaFile string
	isDir, _, err = s.isDirExistsByStorage(rootPathDstDir)
	if err != nil {
		// Initialize new backup
		bkpVersionDir = "V0"
		backupPath = filepath.Join(rootPathDstDir, bkpVersionDir)
		if err := s.createDirByStorage(backupPath, 0755); err != nil {
			return false, err
		}
		*version = 0
	} else {
		if !isDir {
			// Recreate backup directory if it's not a directory
			s.removeDirByStorage(rootPathDstDir)
			bkpVersionDir = "V0"
			backupPath = filepath.Join(rootPathDstDir, bkpVersionDir)
			s.createDirByStorage(backupPath, 0755)
			*version = 0
		} else {
			// Find previous and current backup versions
			prevVer, currVer, err := s.findBackupVersionsByStorage(rootPathDstDir)
			if err != nil {
				return false, err
			}
			bkpVersionDir = fmt.Sprintf("V%v", currVer)
			backupPath = filepath.Join(rootPathDstDir, bkpVersionDir)
			s.createDirByStorage(backupPath, 0755)
			if currVer > 0 {
				incrementalBackup = true
			}
			*version = currVer
			prevBkpVersionDir := fmt.Sprintf("V%v", prevVer)
			prevBkpMetaFile = filepath.Join(rootPathDstDir, prevBkpVersionDir, backupMeta)
		}
	}

	// Set up metadata files
	s.currBackupDstDir = backupPath
	*backupMetaFile = filepath.Join(backupPath, backupMeta)
	*snapMetaFile = filepath.Join(s.metadataDir, snapshotIDFileMap)
	s.backupMetaFilePath = *snapMetaFile
	s.backupListFilePath = filepath.Join(s.metadataDir, backupList)
	*snapShotID = generateMD5Hash([]byte(fmt.Sprintf("%v-V%v", srcDir, *version)))

	// Add root directory metadata
	infoRoot, _ := os.Stat(srcDir)
	s.addBackupMetadata(srcDir, rootPathDstDir, "R", infoRoot, "")
	s.dirCount++

	if !incrementalBackup {
		fmt.Println("Full Backup in progress ....")
		err := s.processFullBackup(srcDir, backupPath, backupHiddenFiles)
		if err != nil {
			return false, err
		}

		// Set up worker pool
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go s.backupWorker(&wg, jobChannel, errors)
		}

		// Collect all jobs
		wg.Add(1)
		go s.collectBackupJobs(srcDir, backupPath, backupHiddenFiles, jobChannel, errors, &wg)

		wg.Wait()
		close(errors)

		return true, nil
	} else {
		fmt.Println("Doing Incremental backup, please wait...")
		err := s.doIncrementalBackup(prevBkpMetaFile, s.currBackupDstDir, *snapMetaFile, *backupMetaFile,
			*snapShotID, srcDir, *version, isEncrypted, backupHiddenFiles)
		return false, err
	}
}

func (s *Sbs) processFullBackup(srcDir, backupPath string, backupHiddenFiles bool) error {
	switch s.storageType {
	case "local", "minio", "aws-s3":
		count := 0
		dirMap := make(map[string]bool)
		return filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if isHidden(path) && !backupHiddenFiles {
				if info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}

			relPath, err := filepath.Rel(srcDir, path)
			if err != nil {
				return err
			}

			dirHash := generateMD5Hash([]byte(relPath))
			dstDir := filepath.Join(backupPath, dirHash[:2])
			_, exists := dirMap[dirHash[:2]]
			if !exists {
				if s.storageType == "local" {
					if err := os.MkdirAll(dstDir, 0755); err != nil {
						return fmt.Errorf("failed to create directory %s: %w", dstDir, err)
					}
				} else {
					if err := s.createDirByStorage(dstDir, 0755); err != nil {
						return err
					}
				}
				dirMap[dirHash[:2]] = true
			}

			if info.IsDir() {
				if relPath != "." {
					dstPath := filepath.Join(dstDir, dirHash)
					if s.storageType == "local" {
						if err := os.MkdirAll(dstPath, 0755); err != nil {
							return fmt.Errorf("failed to create directory %s: %w", dstPath, err)
						}
					} else {
						count++
						if count%256 == 0 {
							fmt.Printf(".")
						}
						if err := s.createDirByStorage(dstPath, 0755); err != nil {
							return err
						}
					}
				}
			} else if !info.Mode().IsRegular() {
				// Skip symlinks and other non-regular files
				return nil
			} else {
				s.totalSize += info.Size()
				s.totalFiles++
			}
			return nil
		})
	case "remote":
		return fmt.Errorf("Storage type %s not implemented", s.storageType)
	default:
		return fmt.Errorf("Unknown storage type: %s", s.storageType)
	}
}

func (s *Sbs) backupWorker(wg *sync.WaitGroup, jobChannel <-chan job, errors chan<- error) {
	defer wg.Done()
	for j := range jobChannel {
		if err := s.processAndStoreFile(j.srcPath, j.dstPath, j.relPath, j.fileInfo); err != nil {
			errors <- err
		}
	}
}

func (s *Sbs) collectBackupJobs(srcDir, backupPath string, backupHiddenFiles bool, jobChannel chan<- job, errors chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(jobChannel)

	err := filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if isHidden(path) && !backupHiddenFiles {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		relPath, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		if relPath == "." {
			return nil
		}

		hashCode := generateMD5Hash([]byte(relPath))
		dstDir := filepath.Join(backupPath, hashCode[:2])
		dstPath := filepath.Join(dstDir, hashCode)

		if info.IsDir() {
			s.dirCount++
			s.addBackupMetadata(relPath, dstPath, "D", info, "")
		} else if info.Mode()&os.ModeSymlink != 0 {
			s.linkCount++
			linkTarget, err := os.Readlink(path)
			if err != nil {
				return fmt.Errorf("failed to read symbolic link %s: %w", path, err)
			}
			s.addBackupMetadata(relPath, dstPath, "L", info, linkTarget)
		} else if info.Mode().IsRegular() {
			s.fileCount++
			jobChannel <- job{srcPath: path, dstPath: dstPath, relPath: relPath, fileInfo: info}
		}
		if atomic.LoadUint32(&s.TaskAborted) == 1 {
			fmt.Println("Aborting Backup Operations...")
			s.Cleanup()
			atomic.StoreUint32(&s.TaskExited, 1)
			os.Exit(1)
		}
		return nil
	})

	if err != nil {
		errors <- err
	}
}

// snapShotMetaFile, backupMetaFile, snapShotID
func (s *Sbs) doIncrementalBackup(prevBkpMetaFile, dstDir, snapshotIDFileMapFile, backupMetaFile, snapShotID,
	srcDir string, version int, isEncrypted bool, backupHiddenFiles bool) error {
	dhMap, err := s.generateDirHashMap(srcDir, backupHiddenFiles)
	if err != nil {
		fmt.Println("Error:", err)
		return err
	}
	err = s.compareSnapEntries(srcDir, dstDir, prevBkpMetaFile, dhMap)
	if err != nil {
		fmt.Println(err)
		return fmt.Errorf("Error restoring snapshot: %v", err)
	}
	err = s.saveBackupMetadataJSONFile(backupMetaFile)
	if err != nil {
		fmt.Printf("Error saving Backup Meta: %v\n", err)
		return nil
	}
	err = s.appendToSnapShotMetadataFile(snapshotIDFileMapFile, backupMetaFile, snapShotID)
	if err != nil {
		fmt.Printf("Error saving Snapshot Meta: %v\n", err)
		return nil
	}
	s.addRecordInBackupListFile(srcDir, version, snapShotID)
	fmt.Printf("\nIncremental Backup completed successfully, Backup ID: %s\n", snapShotID)
	return nil
}

func (s *Sbs) generateDirHashMap(srcDir string, backupHiddenFiles bool) (map[string]BackupRecord, error) {
	dirMap := make(map[string]BackupRecord)
	err := filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if isHidden(path) && !backupHiddenFiles {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		relPath, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		if relPath == "." {
			return nil
		}

		hashCode := generateMD5Hash([]byte(relPath))
		var fileType string
		var linkTarget string

		switch {
		case info.IsDir():
			fileType = "D"
		case info.Mode()&os.ModeSymlink != 0:
			fileType = "L"
			linkTarget, err = os.Readlink(path)
			if err != nil {
				return err
			}
		default:
			fileType = "F"
		}

		record := BackupRecord{
			RelPath:    relPath,
			DstPath:    filepath.Join(hashCode[:2], hashCode),
			Type:       fileType,
			Size:       info.Size(),
			ModTime:    info.ModTime(),
			Permission: info.Mode().Perm(),
			LinkTarget: linkTarget,
		}
		dirMap[relPath] = record
		return nil
	})

	return dirMap, err
}

func (s *Sbs) compareSnapEntries(srcDir, dstRootDir, snapshotFile string, currentDirMap map[string]BackupRecord) error {
	prevDirMap, err := s.loadPreviousSnapshot(snapshotFile)
	if err != nil {
		return err
	}

	// Check for deleted, modified, and unchanged files
	for relPath, prevRecord := range prevDirMap {
		if currentRecord, exists := currentDirMap[relPath]; exists {
			if s.isFileModified(prevRecord, currentRecord) {
				err = s.handleModifiedFile(srcDir, dstRootDir, currentRecord)
			} else {
				s.addBackupMetadataRecord(prevRecord)
			}
		} else {
			// File was deleted
			s.handleDeletedFile(prevRecord)
		}
		if err != nil {
			return err
		}
	}

	// Check for new files
	for relPath, currentRecord := range currentDirMap {
		if _, exists := prevDirMap[relPath]; !exists {
			err = s.handleNewFile(srcDir, dstRootDir, currentRecord)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Sbs) loadPreviousSnapshot(snapshotFile string) (map[string]BackupRecord, error) {
	encryptedData, err := s.readFileByStorage(snapshotFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot file: %v", err)
	}

	decryptedData, err := decrypt(string(encryptedData))
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt snapshot file: %v", err)
	}

	var records []BackupRecord
	err = json.Unmarshal([]byte(decryptedData), &records)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal snapshot content: %v", err)
	}

	prevDirMap := make(map[string]BackupRecord)
	for _, record := range records {
		prevDirMap[record.RelPath] = record
	}

	return prevDirMap, nil
}

func (s *Sbs) isFileModified(prev, curr BackupRecord) bool {
	return prev.Type == "F" && (prev.Size != curr.Size || prev.ModTime != curr.ModTime)
}

func (s *Sbs) handleModifiedFile(srcDir, dstRootDir string, record BackupRecord) error {
	srcPath := filepath.Join(srcDir, record.RelPath)
	dstPath := filepath.Join(dstRootDir, record.DstPath)

	err := s.createDirByStorage(filepath.Dir(dstPath), 0755)
	if err != nil {
		return err
	}

	info, err := os.Lstat(srcPath)
	if err != nil {
		return fmt.Errorf("error getting file info: %v", err)
	}

	return s.processAndStoreFile(srcPath, dstPath, record.RelPath, info)
}

func (s *Sbs) handleDeletedFile(record BackupRecord) {
}

func (s *Sbs) handleNewFile(srcDir, dstRootDir string, record BackupRecord) error {
	srcPath := filepath.Join(srcDir, record.RelPath)
	dstPath := filepath.Join(dstRootDir, record.DstPath)

	err := s.createDirByStorage(filepath.Dir(dstPath), 0755)
	if err != nil {
		return err
	}

	switch record.Type {
	case "D":
		return s.createDirByStorage(dstPath, record.Permission)
	case "L":
		if s.storageType == "local" {
			return os.Symlink(record.LinkTarget, dstPath)
		}
	case "F":
		info, err := os.Lstat(srcPath)
		if err != nil {
			return fmt.Errorf("error getting file info: %v", err)
		}
		return s.processAndStoreFile(srcPath, dstPath, record.RelPath, info)
	}

	return nil
}

func (s *Sbs) restoreWithChecks(srcDir, dstDir, snapshotFile string) error {
	prevDirMap, err := s.loadPreviousSnapshot(snapshotFile)
	if err != nil {
		return err
	}

	currentDirMap, err := s.generateDirHashMap(dstDir, true)
	if err != nil {
		return err
	}

	// Check for extra files in the destination
	for relPath := range currentDirMap {
		if _, exists := prevDirMap[relPath]; !exists {
			fmt.Printf("Extra file found in destination: %s\n", relPath)
		}
	}

	// Check for missing and changed files
	for relPath, prevRecord := range prevDirMap {
		if currentRecord, exists := currentDirMap[relPath]; exists {
			if s.isFileModified(prevRecord, currentRecord) {
				fmt.Printf("File changed: %s\n", relPath)
				err = s.handleModifiedFile(srcDir, dstDir, prevRecord)
			}
		} else {
			fmt.Printf("File missing: %s\n", relPath)
			err = s.handleNewFile(srcDir, dstDir, prevRecord)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

// processAndStoreFile reads a file, applies encryption/deduplication/compression,
// and writes the processed content to the destination path.
func (s *Sbs) processAndStoreFile(srcPath, dstPath, relPath string, info os.FileInfo) error {
	// Read the file content
	fileData, err := os.ReadFile(srcPath)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", srcPath, err)
	}
	// Check for deduplication
	origFile, isDeduplicated := s.deduplicateFiles(fileData, dstPath)
	if isDeduplicated {
		s.duplicateFilesCount++
		s.duplicateFilesSize += info.Size()
		s.addBackupMetadata(relPath, dstPath, "U", info, origFile)
		return nil
	}

	// Process the file data
	dataToWrite, err := s.processFileData(fileData)
	if err != nil {
		return err
	}

	// Write the processed content to the destination file
	if err := s.writeFileByStorage(dstPath, dataToWrite, info.Mode().Perm()); err != nil {
		return fmt.Errorf("failed to write content: %w", err)
	}

	s.addBackupMetadata(relPath, dstPath, "F", info, "")

	if s.isVerboseOutput {
		fmt.Printf("Saved %v\n", srcPath)
	} else {
		s.backupFileCount++
		if s.backupFileCount%32 == 0 {
			fmt.Printf(".")
		}
	}
	return nil
}

// processFileData applies encryption or compression to the file data
func (s *Sbs) processFileData(fileData []byte) ([]byte, error) {
	if s.isEncrypted {
		encryptedData, err := encrypt(fileData)
		if err != nil {
			return nil, fmt.Errorf("failed to encrypt content: %w", err)
		}
		return []byte(encryptedData), nil
	} else if s.isCompressed {
		compressedData, err := s.compressFile(fileData)
		if err != nil {
			return nil, fmt.Errorf("failed to compress content: %w", err)
		}
		return []byte(compressedData), nil
	}
	return fileData, nil
}

func (s *Sbs) RestoreCmd(ctx context.Context, snapshotID, dstDir string, numGoroutines int) error {
	if err := s.validatePassword(); err != nil {
		return err
	}

	s.currentStatus = statusRestore
	runtime.GOMAXPROCS(numGoroutines)

	fmt.Println("Restore in progress...")

	mapFilePath := filepath.Join(s.metadataDir, snapshotIDFileMap)
	snapshotFile, dataOptimization, err := s.getIDMapFilePath(snapshotID, mapFilePath)
	if err != nil {
		return fmt.Errorf("error reading snapshot map file: %w", err)
	}

	err = s.restoreSnapshot(snapshotFile, dstDir, dataOptimization, numGoroutines)
	if err != nil {
		return fmt.Errorf("error restoring snapshot: %w", err)
	}

	fmt.Printf("\nSnapshot %v restored successfully\n", snapshotID)
	return nil
}

func (s *Sbs) validatePassword() error {
	passwordFile := s.passwordFilePath
	if err := s.checkFileExistsByStorage(passwordFile, false); err != nil {
		return fmt.Errorf("password file not found: %v", passwordFile)
	}

	encryptedPassword, err := s.readFileByStorage(passwordFile)
	if err != nil {
		return fmt.Errorf("failed to read encrypted password: %w", err)
	}

	password, err := decrypt(string(encryptedPassword))
	if err != nil {
		return fmt.Errorf("failed to decrypt password: %w", err)
	}

	passwordInput, err := s.readPassword("Enter password: ")
	if err != nil {
		return fmt.Errorf("failed to read password: %w", err)
	}

	if string(password) != passwordInput {
		return fmt.Errorf("password comparison failed")
	}

	return nil
}

func (s *Sbs) restoreSnapshot(snapshotFile, outputDir, dataOptimization string, numGoroutines int) error {
	records, err := s.loadSnapshotRecords(snapshotFile)
	if err != nil {
		return err
	}

	rootDir, err := s.prepareRestoreDirectory(records, outputDir)
	if err != nil {
		return err
	}

	err = s.restoreFiles(records, rootDir, dataOptimization, numGoroutines)
	if err != nil {
		return err
	}

	s.updateDirectoryPermissions(records, rootDir)

	return nil
}

func (s *Sbs) loadSnapshotRecords(snapshotFile string) ([]BackupRecord, error) {
	encryptedData, err := s.readFileByStorage(snapshotFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot file: %w", err)
	}

	decryptedData, err := decrypt(string(encryptedData))
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt snapshot file: %w", err)
	}

	var records []BackupRecord
	err = json.Unmarshal([]byte(decryptedData), &records)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal snapshot content: %w", err)
	}

	return records, nil
}

func (s *Sbs) prepareRestoreDirectory(records []BackupRecord, outputDir string) (string, error) {
	var rootPath string
	for _, record := range records {
		if record.Type == "R" {
			rootDir := record.RelPath
			destinationPath := filepath.Join(outputDir, filepath.Base(rootDir))
			rootPath = destinationPath
			err := os.RemoveAll(destinationPath)
			if err != nil {
				return "", fmt.Errorf("cannot remove the target directory: %v, remove it manually and then rerun restore: %w",
					destinationPath, err)
			}
			err = os.MkdirAll(destinationPath, record.Permission)
			if err != nil {
				return "", fmt.Errorf("failed to create root directory: %w", err)
			}
			s.currRestoreDstDir = destinationPath
		} else if record.Type == "D" {
			relPath := record.RelPath
			destinationPath := filepath.Join(rootPath, relPath)
			os.MkdirAll(destinationPath, 0755)
		}
	}
	return rootPath, nil
}

func (s *Sbs) restoreFiles(records []BackupRecord, rootDir, dataOptimization string, numGoroutines int) error {
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, numGoroutines)
	for _, record := range records {
		if record.Type == "R" || record.Type == "D" || record.Type == "L" {
			continue
		}
		destinationPath := filepath.Join(rootDir, record.RelPath)

		wg.Add(1)
		semaphore <- struct{}{}
		go func(r BackupRecord) {
			defer wg.Done()
			defer func() { <-semaphore }()

			sourcePath := r.DstPath
			if r.Type == "U" {
				sourcePath = r.LinkTarget
			}

			err := s.restoreFile(sourcePath, destinationPath, r.Permission, dataOptimization)
			if err != nil {
				fmt.Printf("Failed to restore file: %v\n", err)
			}
		}(record)

		if atomic.LoadUint32(&s.TaskAborted) == 1 {
			fmt.Println("Aborting Restore Operations...")
			s.Cleanup()
			atomic.StoreUint32(&s.TaskExited, 1)
		}
	}

	wg.Wait()

	for _, record := range records {
		destinationPath := filepath.Join(rootDir, record.RelPath)
		if record.Type == "L" {
			if err := os.Symlink(record.LinkTarget, destinationPath); err != nil {
				return fmt.Errorf("failed to create symbolic link %s -> %s: %w", destinationPath, record.LinkTarget, err)
			}
			continue
		}
	}

	return nil
}

func (s *Sbs) restoreFile(sourcePath, destinationPath string, perm os.FileMode, dataOptimization string) error {
	fileData, err := s.readFileByStorage(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", sourcePath, err)
	}

	if dataOptimization == fileEncrypted {
		decryptedData, err := decrypt(string(fileData))
		if err != nil {
			return fmt.Errorf("failed to decrypt file %s: %w", sourcePath, err)
		}
		fileData = []byte(decryptedData)
	} else if dataOptimization == fileCompressed {
		uncompressedData, err := s.uncompressFile(fileData)
		if err != nil {
			return fmt.Errorf("failed to uncompress file %s: %w", sourcePath, err)
		}
		fileData = uncompressedData
	}

	err = os.WriteFile(destinationPath, fileData, perm)
	if err != nil {
		return fmt.Errorf("failed to write file %s: %w", destinationPath, err)
	}

	if s.isVerboseOutput {
		fmt.Printf("Restored %v\n", destinationPath)
	} else {
		s.restoreFileCount++
		if s.restoreFileCount%32 == 0 {
			fmt.Printf(".")
		}
	}

	return nil
}

func (s *Sbs) updateDirectoryPermissions(records []BackupRecord, rootDir string) {
	for _, record := range records {
		if record.Type == "D" || record.Type == "R" {
			destinationPath := filepath.Join(rootDir, record.RelPath)
			os.Chmod(destinationPath, record.Permission)
		}
	}
}

func (s *Sbs) ListBackupCmd(ctx context.Context) error {
	s.currentStatus = statusListBackup
	return s.listBackupRecords()
}

func (s *Sbs) listBackupRecords() error {
	if err := s.validatePassword(); err != nil {
		return err
	}
	encryptedData, err := s.readFileByStorage(s.backupListFilePath)
	if err != nil {
		return fmt.Errorf("Error reading file:", err, s.backupListFilePath)
	}
	if len(encryptedData) == 0 {
		return fmt.Errorf("Corrupted or incomplete metadata file")
	}
	// Decrypt the snapshot content
	decryptedData, err := decrypt(string(encryptedData))
	if err != nil {
		return fmt.Errorf("Failed to decrypt snapshot file: %v\n", err)
	}
	var records []BackupListRecord
	err = json.Unmarshal([]byte(decryptedData), &records)
	if err != nil {
		return fmt.Errorf("failed to unmarshal snapshot content: %v", err)
	}
	for _, record := range records {
		fmt.Printf("Backup Dir: %v, Version: %v, Backup ID: %v, Timestamp: %v\n",
			record.SourceDirName, record.Version, record.SnapshotID, record.BackupTime)
	}
	return nil
}

func (s *Sbs) readPassword(prompt string) (string, error) {
	fmt.Print(prompt)
	password, err := terminal.ReadPassword(int(os.Stdin.Fd()))
	if err != nil {
		return "", err
	}
	if atomic.LoadUint32(&s.TaskAborted) == 1 {
		fmt.Printf("Aborting %v\n", s.currentStatus)
		s.Cleanup()
		os.Exit(1)
	}
	fmt.Println() // Print a newline after the password input
	return string(password), nil
}

// Read the snapshot map file and return the snapshot file name for a given MD5 hash
func (s *Sbs) getIDMapFilePath(snapshotID string, filePath string) (string, string, error) {
	var records []SnapshotRecord
	// Read the encrypted map file
	encryptedData, err := s.readFileByStorage(filePath)
	if err != nil || (len(encryptedData) == 0) {
		return "", "", fmt.Errorf("getIDMapFilePath: Failed to read snapshot file: %v", err)
	}

	// Decrypt the content
	decryptedData, err := decrypt(string(encryptedData))
	if err != nil {
		return "", "", fmt.Errorf("failed to decrypt snapshot file: %v", err)
	}
	err = json.Unmarshal([]byte(decryptedData), &records)
	if err != nil {
		return "", "", fmt.Errorf("failed to unmarshal snapshot content: %v", err)
	}
	for _, record := range records {
		if record.SnapshotID == snapshotID {
			return record.MetadataFile, record.FileFormat, nil
		}
	}
	return "", "", fmt.Errorf("snapshot file for id not found: %s", snapshotID)
}

func (s *Sbs) appendToSnapShotMetadataFile(snapMetaFile string, backupMeta string, snapShotID string) error {
	var records []SnapshotRecord

	fileExists := true
	err := s.checkFileExistsByStorage(snapMetaFile, false)
	if err != nil {
		if s.storageType != "aws-s3" {
			return err
		}
		fileExists = false
	}

	record := SnapshotRecord{
		SnapshotID:   snapShotID,
		MetadataFile: backupMeta,
	}

	if fileExists {
		encryptedData, err := s.readFileByStorage(snapMetaFile)
		if err != nil {
			return fmt.Errorf("Failed to read snapshot file: %v", err)
		}
		if s.isEncrypted {
			record.FileFormat = fileEncrypted
		} else if s.isCompressed {
			record.FileFormat = fileCompressed
		} else {
			record.FileFormat = fileRegular
		}

		if len(encryptedData) > 0 {
			// Decrypt the content
			decryptedData, err := decrypt(string(encryptedData))
			if err != nil {
				return fmt.Errorf("failed to decrypt snapshot file: %v", err)
			}
			err = json.Unmarshal([]byte(decryptedData), &records)
			if err != nil {
				return fmt.Errorf("failed to unmarshal snapshot content: %v", err)
			}
			records = append(records, record)
		} else {
			records = append(records, record)
		}
	} else {
		records = append(records, record)
	}
	jsonData, err := json.Marshal(records)
	if err != nil {
		return err
	}

	encryptedDataStr, err := encrypt(jsonData)
	if err != nil {
		return err
	}
	encryptedData := []byte(encryptedDataStr)

	// Write the encrypted content to the destination file
	if err := s.writeFileByStorage(snapMetaFile, []byte(encryptedData), 0644); err != nil {
		return fmt.Errorf("failed to write content: %v", err)
	}
	return nil
}

// createDir ensures that the specified directory is created.
func createDir(dirPath string) error {
	if err := os.MkdirAll(dirPath, 0777); err != nil {
		return fmt.Errorf("failed to create directory %s: %v", dirPath, err)
	}
	return nil
}

func remove(slice []string, i int) []string {
	return append(slice[:i], slice[i+1:]...)
}

func (s *Sbs) newBackupRecord(sourceDirName string, version int, snapshotID string) BackupListRecord {
	hash := md5.Sum([]byte(sourceDirName))
	record := BackupListRecord{
		SourceDirName: sourceDirName,
		SnapshotID:    snapshotID,
		Version:       version,
		HashCode:      hex.EncodeToString(hash[:]),
		BackupTime:    time.Now().Format("2006-01-02 15:04:05"),
	}
	return record
}

func (s *Sbs) addRecordInBackupListFile(sourceDirName string, version int, snapshotID string) error {
	var records []BackupListRecord

	fileExists := true
	err := s.checkFileExistsByStorage(s.backupListFilePath, false)
	if err != nil {
		if s.storageType != "aws-s3" {
			return err
		}
		fileExists = false
	}

	record := s.newBackupRecord(sourceDirName, version, snapshotID)

	if fileExists {
		encryptedData, err := s.readFileByStorage(s.backupListFilePath)

		if err != nil {
			return fmt.Errorf("appendBackupRecordToFile: Failed to read snapshot file: %v", err)
		}
		if len(encryptedData) > 0 {
			// Decrypt the snapshot content
			decryptedData, err := decrypt(string(encryptedData))
			if err != nil {
				return fmt.Errorf("failed to decrypt snapshot file: %v", err)
			}
			err = json.Unmarshal([]byte(decryptedData), &records)
			if err != nil {
				return fmt.Errorf("failed to unmarshal snapshot content: %v", err)
			}
			records = append(records, record)
		} else {
			records = append(records, record)
		}
	} else {
		records = append(records, record)
	}

	// Encrypt the content
	jsonData, err := json.Marshal(records)
	if err != nil {
		return err
	}

	encryptedDataStr, err := encrypt(jsonData)
	if err != nil {
		return err
	}
	encryptedData := []byte(encryptedDataStr)
	// Write the encrypted content to the destination file
	if err := s.writeFileByStorage(s.backupListFilePath, []byte(encryptedData), 0644); err != nil {
		return fmt.Errorf("failed to write content: %v", err)
	}
	return nil
}

func processFile(file string, completed *int64, total int64) {
	atomic.AddInt64(completed, 1)
	showProgress(atomic.LoadInt64(completed), total, file)
}

// showProgress displays the progress arrow and the current file being processed
func showProgress(completed int64, total int64, file string) {
	percent := float64(completed) / float64(total) * 100
	progress := int(percent / 2)
	bar := strings.Repeat("=", progress) + ">"
	fmt.Printf("\033[1F\r\033[K[%s] %3.0f%% - %s\n", bar, percent, file)
}

func moveCursor(row, col int) {
	fmt.Printf("\033[%d;%dH", row, col)
}

func clearLine() {
	fmt.Print("\033[2K") // Clear the entire current line
	fmt.Print("\r")      // Move cursor to the beginning of the line
}

func isDirExists(path string) (bool, os.FileMode, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, 0, nil // Path does not exist
		}
		return false, 0, err // Some other error
	}

	if !info.IsDir() {
		return false, 0, fmt.Errorf("%s is not a directory", path)
	}

	return true, info.Mode().Perm(), nil
}

func formatMemoryGB(value float64) string {
	if value >= (1024 * 1024 * 1024) {
		return fmt.Sprintf("%.2fG", value/float64(1024*1024*1024))
	} else if value >= (1024 * 1024) {
		return fmt.Sprintf("%.2fM", value/float64(1024*1024))
	} else if value >= (1024) {
		return fmt.Sprintf("%.2fK", value/float64(1024))
	}
	return fmt.Sprintf("%.2fB", value)
}

func findBackupVersions(dataDir string) (int, int, error) {
	entries, err := ioutil.ReadDir(dataDir)
	if err != nil {
		return 0, 0, err
	}

	var versions []string
	for _, entry := range entries {
		if entry.IsDir() && strings.Contains(entry.Name(), "V") {
			versions = append(versions, entry.Name())
		}
	}
	sort.Strings(versions)

	if len(versions) == 0 {
		return 0, 0, nil
	}

	prevVer := len(versions) - 1
	newVer := len(versions)
	return prevVer, newVer, nil
}

func (s *Sbs) Cleanup() error {
	switch s.currentStatus {
	case statusInit:
		fmt.Printf("Deleting files/directories under %v...\n", s.repoPath)
		if err := s.removeDirByStorage(s.repoPath); err != nil {
			return fmt.Errorf("failed to remove repository directory: %v", err)
		}
	case statusBackup:
		if s.storageType == "minio" && !strings.HasSuffix(s.currBackupDstDir, "/") {
			s.currBackupDstDir += "/"
			removeDirectoryWithMC(s.currBackupDstDir)
		}

		fmt.Printf("Deleting files/directories under %v...\n", s.currBackupDstDir)

		if err := s.removeDirByStorage(s.currBackupDstDir); err != nil {
			return fmt.Errorf("failed to remove backup version directory: %v %v", s.currBackupDstDir, err)
		}
		isDir, _, err := s.isDirExistsByStorage(s.currBackupDstDir)
		if err == nil && isDir {
			fmt.Printf("\nPlease remove backup version directory manually if not already removed: %v\n", s.currBackupDstDir)
		}
	case statusRestore:
		os.RemoveAll(s.currRestoreDstDir)
	case statusListBackup:
	}
	return nil
}

func removeDirectoryWithMC(dirPath string) {
	cmd := exec.Command("mc", "rm", "--recursive", "--force", dirPath)
	cmd.CombinedOutput()
}

func isHidden(path string) bool {
	name := filepath.Base(path)

	if strings.HasPrefix(name, ".") {
		return true
	}

	return false
}

func (s *Sbs) addBackupMetadata(relPath, dstPath, fileType string, fileInfo os.FileInfo, linkTarget string) {
	record := BackupRecord{
		RelPath:    relPath,
		DstPath:    dstPath,
		Type:       fileType,
		Size:       fileInfo.Size(),
		ModTime:    fileInfo.ModTime(),
		Permission: fileInfo.Mode().Perm(),
		LinkTarget: linkTarget,
	}
	s.addBackupMetadataRecord(record)
}

func (s *Sbs) addBackupMetadataRecord(record BackupRecord) {
	s.fileMu.Lock()
	s.backupRecords = append(s.backupRecords, record)
	s.fileMu.Unlock()
}

func (s *Sbs) saveBackupMetadataJSONFile(backupMetaPath string) error {
	jsonData, err := json.Marshal(s.backupRecords)
	if err != nil {
		return err
	}

	encryptedData, err := encrypt(jsonData)
	if err != nil {
		return err
	}
	err = s.writeFileByStorage(backupMetaPath, []byte(encryptedData), 0644)
	if err != nil {
		return err
	}
	return nil
}

// Encrypt encrypts data using AES-256 CFB mode
func encrypt(plaintext []byte) (string, error) {
	key := []byte(encryptionKey)

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	ciphertext := make([]byte, aes.BlockSize+len(plaintext))
	iv := ciphertext[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return "", err
	}

	stream := cipher.NewCFBEncrypter(block, iv)
	stream.XORKeyStream(ciphertext[aes.BlockSize:], plaintext)

	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// Decrypt decrypts data using AES-256 CFB mode
func decrypt(ciphertext string) ([]byte, error) {
	key := []byte(encryptionKey)
	data, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	if len(data) < aes.BlockSize {
		return nil, errors.New("ciphertext too short")
	}
	iv := data[:aes.BlockSize]
	data = data[aes.BlockSize:]

	stream := cipher.NewCFBDecrypter(block, iv)
	stream.XORKeyStream(data, data)

	return data, nil
}

/* Function to create directories from 00 to ff: Hash-based sharding or Prefix sharding, a common
 * technique in distributed systems and file storage to manage large numbers of files efficiently.
 */
func createHexDirs(baseDir string) error {
	for i := 0; i <= 0xff; i++ { // Loop from 0 to 255 (0x00 to 0xff)
		// Format the number as a 2-character hexadecimal string
		dirName := fmt.Sprintf("%02x", i)

		// Create the full path for the directory
		dirPath := filepath.Join(baseDir, dirName)

		// Create the directory with appropriate permissions
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %v", dirPath, err)
		}
	}

	return nil
}

// generateMD5Hash generates an MD5 hash of the string
func generateMD5Hash(content []byte) string {
	hasher := md5.New()
	hasher.Write(content)
	return hex.EncodeToString(hasher.Sum(nil))
}

// Initialize the MinIO client
func getMinioClient(accessKeyID, secretAccessKey, endpoint string) (*minio.Client, error) {
	return minio.New(endpoint, &minio.Options{
		Creds:  miniocredentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: false, // Use HTTP
	})
}

func (s *Sbs) removeDirByStorage(dirPath string) error {
	switch s.storageType {
	case "local":
		if err := os.RemoveAll(dirPath); err != nil {
			return fmt.Errorf("failed to remove local file or directory: %w", err)
		}
	case "remote":
		return fmt.Errorf("remote file removal not implemented")
	case "minio":
		return s.removeDirAllMinio(dirPath)

	case "aws-s3":
		return s.removeDirAllS3(dirPath)

	default:
		return fmt.Errorf("unsupported target: %s", s.storageType)
	}
	return nil
}

func (s *Sbs) createDirMinioS3(dirPath string) error {

	switch s.storageType {
	case "minio":
		objectName := s.getPathBase(dirPath, true) // Ensures the path has a trailing slash
		// Use MinIO client to create a directory
		_, err := s.minioClient.PutObject(context.Background(), s.bucketName, objectName, strings.NewReader(""), 0, minio.PutObjectOptions{})
		if err != nil {
			return fmt.Errorf("failed to create directory in MinIO: %w", err)
		}

	case "aws-s3":
		objectName := s.getPathBaseS3(dirPath, true)
		// Use AWS S3 client to create a directory
		_, err := s.s3Client.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(s.bucketName),
			Key:    aws.String(objectName),
			Body:   bytes.NewReader([]byte{}),
		})
		if err != nil {
			return fmt.Errorf("failed to create directory in S3: %w", err)
		}

	default:
		return fmt.Errorf("unknown storage type: %s", s.storageType)
	}

	return nil
}

func (s *Sbs) copyFileByStorage(localPath, remotePath string) error {
	// Open the local file
	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open local file: %w", err)
	}
	defer file.Close()

	// Get file information
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	switch s.storageType {
	case "local":
		// For local storage, copy the file to a local destination
		destFile, err := os.Create(remotePath)
		if err != nil {
			return fmt.Errorf("failed to create destination file: %w", err)
		}
		defer destFile.Close()

		_, err = file.Seek(0, 0) // Ensure we're reading from the start
		if err != nil {
			return fmt.Errorf("failed to seek file: %w", err)
		}

		_, err = destFile.ReadFrom(file)
		if err != nil {
			return fmt.Errorf("failed to copy file locally: %w", err)
		}

	case "minio":
		objectName := s.getPathBase(remotePath, false) // Get object key (remote path)
		// Use MinIO client to upload file
		_, err = s.minioClient.PutObject(context.Background(), s.bucketName, objectName, file, fileInfo.Size(), minio.PutObjectOptions{})
		if err != nil {
			return fmt.Errorf("failed to upload file to MinIO: %w", err)
		}

	case "aws-s3":
		// Use AWS S3 client to upload file
		objectName := s.getPathBaseS3(remotePath, false) // Get object key (remote path)
		_, err = s.s3Client.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(s.bucketName),
			Key:    aws.String(objectName),
			Body:   file,
		})
		if err != nil {
			return fmt.Errorf("failed to upload file to S3: %w", err)
		}

	case "remote":
		// Placeholder for a remote copy process (e.g., via SSH, FTP, etc.)
		return fmt.Errorf("remote file copy is not implemented")

	default:
		return fmt.Errorf("unknown storage type: %s", s.storageType)
	}

	return nil
}

func (s *Sbs) removeDirAllMinio(dirPath string) error {
	client := s.minioClient
	objectName := s.getPathBase(dirPath, true)

	objectCh := client.ListObjects(context.Background(), s.bucketName, minio.ListObjectsOptions{
		Prefix:    objectName,
		Recursive: true,
	})

	for object := range objectCh {
		if object.Err != nil {
			return fmt.Errorf("failed to list objects: %w", object.Err)
		}

		// Remove each object (file, sub-directory, or symbolic link)
		err := client.RemoveObject(context.Background(), s.bucketName, object.Key, minio.RemoveObjectOptions{})
		if err != nil {
			return fmt.Errorf("failed to remove object %s: %w", object.Key, err)
		}
	}

	// Remove the directory object itself (if it exists as an empty directory object)
	err := client.RemoveObject(context.Background(), s.bucketName, objectName, minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to remove directory %s: %w", objectName, err)
	}

	return nil
}

func (s *Sbs) removeDirAllS3(dirPath string) error {
	svc := s.s3Client

	objectName := s.getPathBaseS3(dirPath, true)
	// Prepare to list objects
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucketName),
		Prefix: aws.String(objectName),
	}

	// List objects with the specified prefix
	result, err := svc.ListObjectsV2(input)
	if err != nil {
		return fmt.Errorf("failed to list objects: %w", err)
	}

	// Remove each object
	for _, obj := range result.Contents {
		_, err := svc.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(s.bucketName),
			Key:    obj.Key,
		})
		if err != nil {
			return fmt.Errorf("failed to remove object %s: %w", *obj.Key, err)
		}
	}

	// Optionally, remove the directory object itself if it exists
	_, err = svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(objectName),
	})
	if err != nil {
		return fmt.Errorf("failed to remove directory %s: %w", objectName, err)
	}

	return nil
}

func (s *Sbs) createDirByStorage(dirPath string, perm fs.FileMode) error {
	switch s.storageType {
	case "local":
		return os.MkdirAll(dirPath, perm)
	case "remote":
		return fmt.Errorf("remote directory creation not implemented")
	case "minio":
		return s.createDirMinioS3(dirPath)
	case "aws-s3":
		return s.createDirMinioS3(dirPath)
	default:
		return fmt.Errorf("unsupported backend: %s", s.storageType)
	}
	return nil
}

func (s *Sbs) writeFileByStorage(path string, data []byte, perm fs.FileMode) error {
	switch s.storageType {
	case "local":
		// Write file locally using os.WriteFile
		if err := os.WriteFile(path, data, perm); err != nil {
			return fmt.Errorf("failed to write file locally: %v", err)
		}

	case "minio":
		// Write file to MinIO storage
		objectName := s.getPathBase(path, false)
		dataReader := bytes.NewReader(data)
		_, err := s.minioClient.PutObject(context.Background(), s.bucketName, objectName, dataReader, int64(len(data)), minio.PutObjectOptions{
			ContentType: "application/octet-stream",
		})
		if err != nil {
			return fmt.Errorf("failed to upload file to MinIO: %v", err)
		}

	case "aws-s3":
		// Write file to AWS S3
		objectName := s.getPathBaseS3(path, false)
		dataReader := bytes.NewReader(data)
		_, err := s.s3Client.PutObject(&s3.PutObjectInput{
			Bucket:        aws.String(s.bucketName),
			Key:           aws.String(objectName),
			Body:          dataReader,
			ContentLength: aws.Int64(int64(len(data))),
			ContentType:   aws.String("application/octet-stream"),
		})
		if err != nil {
			return fmt.Errorf("failed to upload file to AWS S3: %v", err)
		}

	default:
		return fmt.Errorf("unsupported backend: %s", s.storageType)
	}

	return nil
}

func (s *Sbs) openFileByStorage(path string) error {
	switch s.storageType {
	case "local":
		file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("failed to open file: %v", err)
		}
		defer file.Close()

	case "remote":
		return fmt.Errorf("Remote file system is not implemented")

	case "minio":
		objectName := s.getPathBase(path, false)
		// Create or append a file in MinIO by uploading a nil reader
		_, err := s.minioClient.PutObject(context.Background(), s.bucketName, objectName, nil, 0, minio.PutObjectOptions{
			ContentType: "application/octet-stream",
		})
		if err != nil {
			return fmt.Errorf("failed to create or append file in MinIO: %v", err)
		}

	case "aws-s3":
		// Fetch the file from S3
		objectName := s.getPathBaseS3(path, false)

		result, err := s.s3Client.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(s.bucketName),
			Key:    aws.String(objectName),
		})
		if err != nil {
			return fmt.Errorf("failed to get file from S3 (bucket: %s, key: %s): %v", s.bucketName, objectName, err)
		}
		defer result.Body.Close()

		// Ensure the directory exists
		dir := filepath.Dir(path)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory: %v", err)
		}

		// Create a local file to write the S3 object
		localFile, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("failed to create local file: %v", err)
		}
		defer localFile.Close()

		// Copy the data from the S3 object to the local file
		_, err = io.Copy(localFile, result.Body)
		if err != nil {
			return fmt.Errorf("failed to copy data from S3 to local file: %v", err)
		}

	default:
		return fmt.Errorf("unsupported storage type: %s", s.storageType)
	}
	return nil
}

func (s *Sbs) checkFileExistsByStorage(filePath string, isDirectory bool) error {
	var objectName string
	switch s.storageType {
	case "local":
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			return fmt.Errorf("file not found: %v", filePath)
		}

	case "remote":
		return fmt.Errorf("remote file system is not implemented")

	case "minio":
		objectName = s.getPathBase(filePath, isDirectory)
		_, err := s.minioClient.StatObject(context.Background(), s.bucketName, objectName, minio.StatObjectOptions{})
		if err != nil {
			if minio.ToErrorResponse(err).Code == "NoSuchKey" {
				return fmt.Errorf("file not found in MinIO: %s", objectName)
			}
			return fmt.Errorf("error checking file in MinIO: %v", err)
		}

	case "aws-s3":
		objectName = s.getPathBaseS3(filePath, isDirectory)
		_, err := s.s3Client.HeadObject(&s3.HeadObjectInput{
			Bucket: aws.String(s.bucketName),
			Key:    aws.String(objectName),
		})
		if err != nil {
			return fmt.Errorf("error checking file in S3: %v", err)
		}

	default:
		return fmt.Errorf("unsupported storage type: %s", s.storageType)
	}

	return nil
}

func (s *Sbs) readFileByStorage(filePath string) ([]byte, error) {
	switch s.storageType {
	case "local":
		content, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read file: %v %v", filePath, err)
		}
		return content, nil

	case "remote":
		return nil, fmt.Errorf("remote file system is not implemented")

	case "minio":
		objectName := s.getPathBase(filePath, false)
		object, err := s.minioClient.GetObject(context.Background(), s.bucketName, objectName, minio.GetObjectOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to get object from MinIO: %v %v", filePath, err)
		}
		defer object.Close()

		content, err := ioutil.ReadAll(object)
		if err != nil {
			return nil, fmt.Errorf("failed to read file from MinIO: %v %v", filePath, err)
		}
		return content, nil

	case "aws-s3":
		objectName := s.getPathBaseS3(filePath, false)
		result, err := s.s3Client.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(s.bucketName),
			Key:    aws.String(objectName),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to get object from S3: %v %v", filePath, err)
		}
		defer result.Body.Close()

		content, err := ioutil.ReadAll(result.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read file from S3: %v %v", filePath, err)
		}
		return content, nil

	default:
		return nil, fmt.Errorf("unsupported storage type: %s", s.storageType)
	}
}

func (s *Sbs) isDirExistsByStorage(path string) (bool, os.FileInfo, error) {
	switch s.storageType {
	case "local":
		// Check if directory exists in the local filesystem
		fileInfo, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				return false, nil, fmt.Errorf("local path does not exist: %v", path)
			}
			return false, nil, err
		}

		if !fileInfo.IsDir() {
			return false, fileInfo, fmt.Errorf("local path is not a directory: %v", path)
		}
		return true, fileInfo, nil

	case "remote":
		return false, nil, fmt.Errorf("remote file system is not implemented")

	case "minio":
		objectName := s.getPathBase(path, true)
		found := false
		for obj := range s.minioClient.ListObjects(context.Background(), s.bucketName, minio.ListObjectsOptions{
			Prefix:    objectName,
			Recursive: false,
		}) {
			if obj.Err != nil {
				return false, nil, fmt.Errorf("error while listing objects in MinIO: %v", obj.Err)
			}
			if strings.HasPrefix(obj.Key, objectName) {
				found = true
				break
			}
		}
		if !found {
			return false, nil, fmt.Errorf("MinIO directory does not exist: %v", path)
		}
		return true, nil, nil

	case "aws-s3":
		// Check if the backup directory exists in S3
		prefix := s.getPathBaseS3(path, true) // Use the prefix to check the directory
		input := &s3.ListObjectsV2Input{
			Bucket:  aws.String(s.bucketName),
			Prefix:  aws.String(prefix),
			MaxKeys: aws.Int64(1), // Limit to 1 object to check for existence
		}

		result, err := s.s3Client.ListObjectsV2(input)
		if err != nil {
			return false, nil, fmt.Errorf("error listing objects in S3: %v", err)
		}

		if len(result.Contents) > 0 {
			// If we found at least one object with the given prefix, the directory exists
			return true, nil, nil
		}

		return false, nil, fmt.Errorf("AWS S3 directory does not exist: %v", path)

	default:
		return false, nil, fmt.Errorf("unsupported storage type: %s", s.storageType)
	}
}

func (s *Sbs) findBackupVersionsByStorage(dataDir string) (int, int, error) {
	switch s.storageType {
	case "local":
		return findBackupVersions(dataDir)
	case "remote":
		return 0, 0, fmt.Errorf("remote file system is not implemented")
	case "minio":
		return s.findBackupVersionsMinio(dataDir)
	case "aws-s3":
		return s.findBackupVersionsS3(dataDir)
	}
	return 0, 0, fmt.Errorf("Invalid Storage type")
}

func (s *Sbs) findBackupVersionsMinio(dataDir string) (int, int, error) {
	objectName := s.getPathBase(dataDir, true)
	objectCh := s.minioClient.ListObjects(context.Background(), s.bucketName, minio.ListObjectsOptions{
		Prefix:    objectName,
		Recursive: false,
	})

	var versions []string
	for object := range objectCh {
		if object.Err != nil {
			return 0, 0, fmt.Errorf("error listing objects in MinIO: %v", object.Err)
		}

		objectName = strings.TrimPrefix(object.Key, dataDir)
		if strings.Contains(objectName, "V") {
			versions = append(versions, objectName)
		}
	}

	sort.Strings(versions)

	if len(versions) == 0 {
		return 0, 0, nil
	}

	prevVer := len(versions) - 1
	newVer := len(versions)
	return prevVer, newVer, nil
}

func (s *Sbs) findBackupVersionsS3(dataDir string) (int, int, error) {
	// Create S3 service client
	svc := s.s3Client

	// Prepare to list objects in the bucket
	objectName := s.getPathBaseS3(dataDir, true)
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucketName),
		Prefix: aws.String(objectName),
	}

	// List objects
	result, err := svc.ListObjectsV2(input)
	if err != nil {
		return 0, 0, fmt.Errorf("error listing objects in S3: %v", err)
	}

	var versions []int
	for _, obj := range result.Contents {
		// Get the object key
		versionName := *obj.Key
		versionName = filepath.Base(versionName)

		// Check if the object key starts with "V" followed by a number
		if strings.HasPrefix(versionName, "V") {
			versionPart := strings.TrimPrefix(versionName, "V") // Remove "V" prefix
			versionNumber, err := strconv.Atoi(versionPart)     // Convert to integer
			if err == nil {
				versions = append(versions, versionNumber)
			}
		}
	}

	// Sort the versions in ascending order
	sort.Ints(versions)

	if len(versions) == 0 {
		return 0, 0, nil // No versions found
	}

	// Get the previous version and the new version
	prevVer := versions[len(versions)-1] // Last version in the sorted list
	newVer := prevVer + 1                // Next version number

	return prevVer, newVer, nil
}

func (s *Sbs) walkMinio(bucketName, srcDir string, walkFn func(path string, info ExtendedObjectInfo, err error) error) error {
	ctx := context.Background()
	objectCh := s.minioClient.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		Prefix:    srcDir,
		Recursive: true,
	})

	seenDirs := make(map[string]bool)

	for object := range objectCh {
		if object.Err != nil {
			return walkFn(object.Key, ExtendedObjectInfo{}, object.Err)
		}

		objType := determineObjectType(object)

		extendedInfo := ExtendedObjectInfo{
			ObjectInfo: object,
			Type:       objType,
		}

		dirs := strings.Split(strings.TrimPrefix(object.Key, srcDir), "/")
		currentPath := srcDir
		for _, dir := range dirs[:len(dirs)-1] {
			currentPath = filepath.Join(currentPath, dir)
			if !seenDirs[currentPath] {
				seenDirs[currentPath] = true
				dirInfo := ExtendedObjectInfo{
					ObjectInfo: minio.ObjectInfo{Key: currentPath + "/"},
					Type:       TypeDirectory,
				}
				if err := walkFn(currentPath, dirInfo, nil); err != nil {
					return err
				}
			}
		}

		if err := walkFn(object.Key, extendedInfo, nil); err != nil {
			return err
		}
	}

	return nil
}

func determineObjectType(info minio.ObjectInfo) ObjectType {
	if info.Size == 0 && strings.HasSuffix(info.Key, "/") {
		return TypeDirectory
	}
	if linkTarget, ok := info.UserMetadata["X-Amz-Meta-Symlink-Target"]; ok && linkTarget != "" {
		return TypeLink
	}
	return TypeFile
}

func (s *Sbs) minioWalkFnCreateDir(path string, info ExtendedObjectInfo, err error) error {
	if err != nil {
		return err
	}
	relPath, _ := filepath.Rel(s.minioSrcDir, path)
	dirHash := generateMD5Hash([]byte(relPath))
	dstDir := filepath.Join(s.currBackupDstDir, dirHash[:2])
	s.createDirByStorage(dstDir, 0755)
	if info.Type == TypeDirectory {
		if relPath == "." {
			return nil
		}
		dstPath := filepath.Join(dstDir, dirHash)
		s.createDirByStorage(dstPath, 0755)
	} else {
		s.totalSize += info.Size
		s.totalFiles++
	}

	return nil
}

/*  For Minio File system, the path normally can be "<myminio alias>/<bucket name>/<>/..."
 *  We need get the string after bucketname. For directory, it should end with "/"
 */
func (s *Sbs) getPathBase(path string, isDirectory bool) string {
	// Trim any trailing slashes
	path = strings.TrimRight(path, "/")

	// Split the path into parts
	parts := strings.Split(path, "/")

	// Find the bucket name index
	bucketIndex := -1
	for i, part := range parts {
		if part == s.bucketName {
			bucketIndex = i
			break
		}
	}

	var result string
	if bucketIndex == -1 {
		// If bucket name not found, use the last part of the path
		result = parts[len(parts)-1]
	} else {
		// Join the parts after the bucket name
		result = strings.Join(parts[bucketIndex+1:], "/")
	}

	// Add a trailing slash for directories
	if isDirectory && result != "" && !strings.HasSuffix(result, "/") {
		result += "/"
	}

	return result
}

func (s *Sbs) getPathBaseS3(path string, isDir bool) string {
	if isDir {
		if strings.HasSuffix(path, "/") {
			return path
		} else {
			return path + "/"
		}
	} else {
		if strings.HasSuffix(path, "/") {
			return path[0 : len(path)-1]
		}
	}
	return path
}

func (s *Sbs) deduplicateFiles(data []byte, path string) (string, bool) {
	// Calculate hash for deduplication
	hash := sha256.Sum256(data)
	hashStr := hex.EncodeToString(hash[:])

	// Check if this data block already exists
	origFile, isDuplicate := s.blockExists(hashStr, path)
	if isDuplicate {
		return origFile, true
	}

	return "", false
}

func (s *Sbs) compressFile(data []byte) ([]byte, error) {
	var compressedData bytes.Buffer
	gzipWriter := gzip.NewWriter(&compressedData)
	_, err := gzipWriter.Write(data)
	if err != nil {
		return nil, err
	}
	gzipWriter.Close()

	return compressedData.Bytes(), nil
}

func (s *Sbs) uncompressFile(compressedData []byte) ([]byte, error) {
	bytesReader := bytes.NewReader(compressedData)

	gzipReader, err := gzip.NewReader(bytesReader)
	if err != nil {
		return nil, err
	}
	defer gzipReader.Close()

	uncompressedData, err := io.ReadAll(gzipReader)
	if err != nil {
		return nil, err
	}

	return uncompressedData, nil
}

func (s *Sbs) blockExists(hash string, path string) (string, bool) {
	s.fileMu.Lock()
	defer s.fileMu.Unlock()
	_, exists := s.blockHashMap[hash]
	if !exists {
		s.blockHashMap[hash] = path
		return "", false
	}
	return s.blockHashMap[hash], true
}

func (s *Sbs) createFileS3(bucket, key string, content []byte) error {
	_, err := s.s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(content),
		// Set metadata and ACLs as needed
		ACL: aws.String("private"), // Optional: Set ACL to private
	})
	if err != nil {
		return fmt.Errorf("failed to upload file to S3: %v", err)
	}
	return nil
}

func (s *Sbs) verifyAndCreateBucket(bucket, region string) error {
	found, err := s.minioClient.BucketExists(context.Background(), bucket)
	if err != nil {
		return err
	}
	if found {
		return nil
	} else {
		err = s.minioClient.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{Region: region, ObjectLocking: true})
		if err != nil {
			return err
		}
	}
	return nil
}
