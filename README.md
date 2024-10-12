A simple backup system with capabilities:
	- Full backup
	- Incremental backup 
	- Restore from a given backup id
	- List backups
	- Minio Object Store 

./sbs init -r /tmp/.repopath

./sbs backup -s ~/work -r /tmp/.repopath

./sbs restore -i $1 -d /tmp/restore -r /tmp/.repopath 

./sbs backup -s ~/work -r /tmp/.repopath

./sbs list-backup -r /tmp/.repopath

./sbs backup -s ~/work -r /tmp/.repopath -e <enable encryption>

# Minio Support
./sbs init -r myminio/mybucket/.repopath -t minio

# AWS-S3 Support
./sbs init -r s3://mybucket/.repopath -t aws-s3
