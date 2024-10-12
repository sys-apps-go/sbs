package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/sys-apps-go/sbs/internal"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "sbs",
		Usage: "Simple Backup System",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "v",
				Aliases: []string{"verbose"},
				Usage:   "Print detailed output",
			},
		},
		Commands: []*cli.Command{
			{
				Name:   "init",
				Usage:  "Initialize a new repository",
				Action: runInit,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "r",
						Aliases:  []string{"repo"},
						Usage:    "Repository path to store encrypted/compressed/metadata files",
						Required: true,
					},
					&cli.StringFlag{
						Name:    "t",
						Aliases: []string{"type"},
						Value:   "local",
						Usage:   "Storage type: local/remote/minio/s3",
					},
					&cli.StringFlag{
						Name:  "minio-endpoint",
						Value: "http://localhost:9000",
						Usage: "MinIO server endpoint",
					},
					&cli.StringFlag{
						Name:  "aws-region",
						Value: "us-east-1",
						Usage: "AWS Region",
					},
					&cli.StringFlag{
						Name:  "access-key-id",
						Value: "minioadmin",
						Usage: "MinIO/AWS-S3 access key",
					},
					&cli.StringFlag{
						Name:  "secret-access-key",
						Value: "minioadmin",
						Usage: "MinIO/AWS-S3 secret key",
					},
				},
			},
			{
				Name:   "backup",
				Usage:  "Create a new backup",
				Action: runBackup,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "r",
						Aliases:  []string{"repo"},
						Usage:    "Repository path to store encrypted/compressed/metadata files",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "s",
						Aliases:  []string{"source"},
						Usage:    "Source dir to backup",
						Required: true,
					},
					&cli.BoolFlag{
						Name:    "e",
						Aliases: []string{"encrypt"},
						Usage:   "Encrypt the files",
					},
					&cli.BoolFlag{
						Name:    "c",
						Aliases: []string{"compress"},
						Usage:   "Compress the files",
					},
					&cli.BoolFlag{
						Name:    "H",
						Aliases: []string{"hidden"},
						Usage:   "Backup hidden files",
					},
					&cli.IntFlag{
						Name:    "n",
						Aliases: []string{"goroutines"},
						Value:   32,
						Usage:   "Number of Goroutines for backup and restore",
					},
					&cli.StringFlag{
						Name:    "t",
						Aliases: []string{"type"},
						Value:   "local",
						Usage:   "Storage type: local/remote/minio/s3",
					},
					&cli.BoolFlag{
						Name:    "v",
						Aliases: []string{"verbose"},
						Usage:   "Print detailed output",
					},
					&cli.StringFlag{
						Name:  "minio-endpoint",
						Value: "http://localhost:9000",
						Usage: "MinIO server endpoint",
					},
					&cli.StringFlag{
						Name:  "aws-region",
						Value: "us-east-1",
						Usage: "AWS Region",
					},
					&cli.StringFlag{
						Name:  "access-key-id",
						Value: "minioadmin",
						Usage: "MinIO/AWS-S3 access key",
					},
					&cli.StringFlag{
						Name:  "secret-access-key",
						Value: "minioadmin",
						Usage: "MinIO/AWS-S3 secret key",
					},
				},
			},
			{
				Name:   "restore",
				Usage:  "Restore from a backup",
				Action: runRestore,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "r",
						Aliases:  []string{"repo"},
						Usage:    "Repository path to store encrypted/compressed/metadata files",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "i",
						Aliases:  []string{"id"},
						Usage:    "Snapshot ID",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "d",
						Aliases:  []string{"destination"},
						Usage:    "Destination dir to restore",
						Required: true,
					},
					&cli.IntFlag{
						Name:    "n",
						Aliases: []string{"goroutines"},
						Value:   32,
						Usage:   "Number of Goroutines for backup and restore",
					},
					&cli.StringFlag{
						Name:    "t",
						Aliases: []string{"type"},
						Value:   "local",
						Usage:   "Storage type: local/remote/minio/s3",
					},
					&cli.BoolFlag{
						Name:    "v",
						Aliases: []string{"verbose"},
						Usage:   "Print detailed output",
					},
					&cli.StringFlag{
						Name:  "minio-endpoint",
						Value: "http://localhost:9000",
						Usage: "MinIO server endpoint",
					},
					&cli.StringFlag{
						Name:  "aws-region",
						Value: "us-east-1",
						Usage: "AWS Region",
					},
					&cli.StringFlag{
						Name:  "access-key-id",
						Value: "minioadmin",
						Usage: "MinIO/AWS-S3 access key",
					},
					&cli.StringFlag{
						Name:  "secret-access-key",
						Value: "minioadmin",
						Usage: "MinIO/AWS-S3 secret key",
					},
				},
			},
			{
				Name:   "list-backup",
				Usage:  "List available backups",
				Action: runListBackup,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "r",
						Aliases:  []string{"repo"},
						Usage:    "Repository path to store encrypted/compressed/metadata files",
						Required: true,
					},
					&cli.StringFlag{
						Name:    "t",
						Aliases: []string{"type"},
						Value:   "local",
						Usage:   "Storage type: local/remote/minio/s3",
					},
					&cli.StringFlag{
						Name:  "minio-endpoint",
						Value: "http://localhost:9000",
						Usage: "MinIO server endpoint",
					},
					&cli.StringFlag{
						Name:  "aws-region",
						Value: "us-east-1",
						Usage: "AWS Region",
					},
					&cli.StringFlag{
						Name:  "access-key-id",
						Value: "minioadmin",
						Usage: "MinIO/AWS-S3 access key",
					},
					&cli.StringFlag{
						Name:  "secret-access-key",
						Value: "minioadmin",
						Usage: "MinIO/AWS-S3 secret key",
					},
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
}

func runInit(c *cli.Context) error {
	return runCommand(c, func(s *internal.Sbs, ctx context.Context) error {
		return s.InitCmd(ctx)
	})
}

func runBackup(c *cli.Context) error {
	return runCommand(c, func(s *internal.Sbs, ctx context.Context) error {
		return s.BackupCmd(ctx, c.String("s"), c.Bool("e"), c.Bool("c"), c.Bool("H"), c.Int("n"))
	})
}

func runRestore(c *cli.Context) error {
	return runCommand(c, func(s *internal.Sbs, ctx context.Context) error {
		return s.RestoreCmd(ctx, c.String("i"), c.String("d"), c.Int("n"))
	})
}

func runListBackup(c *cli.Context) error {
	return runCommand(c, func(s *internal.Sbs, ctx context.Context) error {
		return s.ListBackupCmd(ctx)
	})
}

func runCommand(c *cli.Context, cmdFunc func(*internal.Sbs, context.Context) error) error {
	repoPath := c.String("r")
	storageType := c.String("t")
	minioEndpoint := c.String("minio-endpoint")
	awsRegion := c.String("aws-region")
	accessKeyID := c.String("access-key-id")
	secretAccessKey := c.String("secret-access-key")

	s := internal.NewBackupSystem(storageType)
	isVerboseOutput := c.Bool("v")

	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Recovered from panic: %v\n", r)
			s.Cleanup()
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	setupSignalHandler(cancel, s)

	if storageType == "minio" {
		minioEndpoint = strings.TrimPrefix(minioEndpoint, "http://")
	}
	err := s.InitializeData(repoPath, storageType, accessKeyID, secretAccessKey, minioEndpoint, awsRegion, isVerboseOutput)
	if err != nil {
		return err
	}

	return cmdFunc(s, ctx)
}

func setupSignalHandler(cancel context.CancelFunc, s *internal.Sbs) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Received interrupt signal. Cancelling operations...")
		s.AbortFlag = true
		cancel()
	}()
}
