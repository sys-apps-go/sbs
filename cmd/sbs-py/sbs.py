import os
import re
import argparse
import json
import shutil
import hashlib
from datetime import datetime

BACKUP_DIR_NAME = "backup"
METADATA_DIR_NAME = "metadata"
BACKUP_META = "snapshot.meta"
SNAPSHOT_ID_FILE_MAP = "snapshot.id"
BACKUP_LIST = "backup.list"


def get_filename_for_snapshot_id(snapshot_id_map_file_path, snapshot_id):
    # Check if the snapshot_id_map_file exists and is not empty
    if (
        os.path.exists(snapshot_id_map_file_path)
        and os.path.getsize(snapshot_id_map_file_path) > 0
    ):
        try:
            # Read the existing content from the file
            with open(snapshot_id_map_file_path, "r") as snapshot_id_file:
                snapshot_id_data = json.load(snapshot_id_file)
        except json.JSONDecodeError:
            snapshot_id_data = []  # Default to empty list or handle as needed
    else:
        snapshot_id_data = []  # Default to empty list or handle as needed

    # Iterate through the outer list and then through the inner list to find the snapshot ID
    for sublist in snapshot_id_data:
        for entry in sublist:
            if entry.get("snapshot_id") == snapshot_id:
                return entry.get(
                    "file_path", None
                )  # Return the filename if found, or None if not present

    # Return None if the snapshot ID is not found
    return None


def update_snapshot_id_file(snapshot_id_file_map, new_entry):
    if (
        os.path.exists(snapshot_id_file_map)
        and os.path.getsize(snapshot_id_file_map) > 0
    ):
        try:
            with open(snapshot_id_file_map, "r") as snapshot_id_file:
                snapshot_id_data = json.load(snapshot_id_file)
        except json.JSONDecodeError as e:
            snapshot_id_data = []  # Default to empty list or handle as needed
    else:
        snapshot_id_data = []  # Default to empty list or handle as needed

    snapshot_id_data.append(new_entry)

    # Write the updated content back to the file
    with open(snapshot_id_file_map, "w") as snapshot_id_file:
        json.dump(snapshot_id_data, snapshot_id_file, indent=4)


def get_versions(path):
    # Regular expression pattern for directories named V0, V1, etc.
    version_pattern = re.compile(r"^V(\d+)$")

    # List all directories in the path
    all_dirs = [d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))]

    # Extract version numbers from directory names
    versions = []
    for d in all_dirs:
        match = version_pattern.match(d)
        if match:
            versions.append(int(match.group(1)))

    # Debugging line to show extracted versions

    # Sort versions
    versions.sort()

    # Debugging line to show sorted versions

    if len(versions) == 0:
        return None, "V0"

    # More than zero versions
    current_version = "V" + str(len(versions))
    previous_version = "V" + str(len(versions) - 1)
    return previous_version, current_version


def touch_file(file_path):
    """Create a file if it doesn't exist, and update its modification time."""
    # Ensure the directory exists
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Create the file if it doesn't exist
    if not os.path.exists(file_path):
        with open(file_path, "a"):
            os.utime(file_path, None)
    else:
        # Update the modification time of the file
        os.utime(file_path, None)


def calculate_md5(path):
    hash_md5 = hashlib.md5()
    hash_md5.update(path.encode("utf-8"))
    return hash_md5.hexdigest()


def do_backup(source_dir, repopath):
    if not os.path.exists(source_dir) or not os.path.isdir(source_dir):
        print(f"Source directory {source_dir} does not exist or is not a directory.")
        return

    version = 0
    hashCode = calculate_md5(source_dir)
    backup_dir = os.path.join(repopath, BACKUP_DIR_NAME)
    metadata_dir = os.path.join(repopath, METADATA_DIR_NAME)
    snapshot_id_map_file = os.path.join(metadata_dir, SNAPSHOT_ID_FILE_MAP)
    metadata_backup_list_file = os.path.join(metadata_dir, BACKUP_LIST)
    destination_root = os.path.join(backup_dir, hashCode)
    snapshot_id = ""
    if not os.path.exists(repopath):
        os.makedirs(repopath)
        os.makedirs(backup_dir)
        os.makedirs(metadata_dir)

        touch_file(snapshot_id_map_file)
        touch_file(metadata_backup_list_file)
    else:
        if not os.path.isdir(repopath):
            shutil.rmtree(repopath)
        touch_file(snapshot_id_map_file)
        touch_file(metadata_backup_list_file)

    if not os.path.exists(destination_root):
        os.makedirs(destination_root)
    else:
        if not os.path.isdir(destination_root):
            shutil.rmtree(destination_root)
            os.makedirs(destination_root)

    pv, cv = get_versions(destination_root)
    if pv is None:
        version = 0
    else:
        version = int(cv[1:])  # cv is of the form "V1", "V2", etc.

    destination_path = os.path.join(destination_root, cv)
    snapshot_id = calculate_md5(destination_path)
    os.makedirs(destination_path)
    metadata_file_path = os.path.join(destination_path, BACKUP_META)
    touch_file(metadata_file_path)
    if version != 0:
        prev_destination_path = os.path.join(destination_root, pv)
        prev_metadata_file_path = os.path.join(prev_destination_path, BACKUP_META)
    curr_metadata = []
    prev_metadata = []
    snapshot_id_file_map = []
    link_name = ""
    if version == 0:
        for root, dirs, files in os.walk(source_dir):
            dirs[:] = [d for d in dirs if not d.startswith(".")]
            rel_path = os.path.relpath(root, source_dir)
            dst_path = os.path.join(destination_path, rel_path)
            os.makedirs(dst_path, exist_ok=True)

            # Process directories
            for d in dirs:
                link_name = ""
                src_path = os.path.join(root, d)
                rel_path = os.path.relpath(src_path, source_dir)
                dst_path = os.path.join(destination_path, rel_path)

                if os.path.islink(src_path):
                    linkto = os.readlink(src_path)
                    os.symlink(linkto, dst_path)
                    file_type = "DL"
                    link_name = linkto
                else:
                    file_type = "D"
                    os.makedirs(dst_path, exist_ok=True)

                curr_metadata.append(
                    {
                        "relative_path": rel_path,
                        "destination_path": dst_path,
                        "file_type": file_type,
                        "link_to": link_name,
                        "file_size": None,
                        "modification_time": datetime.fromtimestamp(
                            os.path.getmtime(src_path)
                        ).isoformat(),
                    }
                )

            # Process files
            for file in files:
                if file.startswith("."):
                    continue
                link_name = ""
                src_path = os.path.join(root, file)
                rel_path = os.path.relpath(src_path, source_dir)
                dst_path = os.path.join(destination_path, rel_path)
                print(src_path, dst_path)

                if os.path.islink(src_path):
                    linkto = os.readlink(src_path)  # /tmp/1/link2 -> file1
                    os.symlink(linkto, dst_path)
                    file_type = "FL"
                    file_size = None
                    link_name = linkto
                else:
                    print(src_path, dst_path)
                    shutil.copy2(src_path, dst_path)
                    file_type = "F"
                    file_size = os.path.getsize(dst_path)

                # print(rel_path, file_type)
                curr_metadata.append(
                    {
                        "relative_path": rel_path,
                        "destination_path": dst_path,
                        "link_to": link_name,
                        "file_type": file_type,
                        "file_size": file_size,
                        "modification_time": datetime.fromtimestamp(
                            os.path.getmtime(src_path)
                        ).isoformat(),
                    }
                )

    else:  # Already level 0 backup done, this is incremental backup
        # 1.Read Metadata file
        if (
            os.path.exists(prev_metadata_file_path)
            and os.path.getsize(prev_metadata_file_path) > 0
        ):
            try:
                with open(prev_metadata_file_path, "r") as file:
                    prev_metadata = json.load(file)
            except json.JSONDecodeError as e:
                print(f"Error loading JSON from {prev_metadata_file_path}: {e}")
                prev_metadata = []

        # 2.Create a Metadata dictionary
        metadata_dict = {entry["relative_path"]: entry for entry in prev_metadata}

        # 3.Look from the new directory to previous directory: There can be:
        # - New file
        # - Modified file
        # - Deleted file
        for root, dirs, files in os.walk(source_dir):
            dirs[:] = [d for d in dirs if not d.startswith(".")]
            # 4. First check all directories that are either real or links
            for d in list(dirs):  # Create a copy of dirs to modify during iteration
                link_name = ""
                src_path = os.path.join(root, d)
                rel_path = os.path.relpath(src_path, source_dir)
                dst_path = os.path.join(destination_path, rel_path)
                # 5. Dir found in prev and current. Copy the metadata
                if rel_path in metadata_dict:
                    curr_metadata.append(metadata_dict[rel_path])
                else:
                    file_type = "D"
                    if os.path.islink(src_path):
                        file_type = "DL"
                        link_name = linkto
                    curr_metadata.append(
                        {
                            "relative_path": rel_path,
                            "destination_path": dst_path,
                            "file_type": file_type,
                            "link_to": link_name,
                            "file_size": None,
                            "modification_time": datetime.fromtimestamp(
                                os.path.getmtime(src_path)
                            ).isoformat(),
                        }
                    )
                if not os.path.islink(src_path):
                    os.makedirs(dst_path, exist_ok=True)
                    # 6. Check, dir/link, is yes use the old record

            for file in list(files):
                if file.startswith("."):
                    continue
                link_name = ""
                src_path = os.path.join(root, file)
                rel_path = os.path.relpath(src_path, source_dir)
                dst_path = os.path.join(destination_path, rel_path)
                if rel_path in metadata_dict:
                    metadata_entry = metadata_dict[rel_path]
                    file_size = os.path.getsize(src_path)
                    file_mod_time = datetime.fromtimestamp(
                        os.path.getmtime(src_path)
                    ).isoformat()
                    if not os.path.islink(src_path):
                        if (
                            metadata_entry["file_size"] != file_size
                            or metadata_entry["modification_time"] != file_mod_time
                        ):
                            shutil.copy2(src_path, dst_path)
                            curr_metadata.append(
                                {
                                    "relative_path": rel_path,
                                    "destination_path": dst_path,
                                    "file_type": "F",
                                    "link_to": link_name,
                                    "file_size": os.path.getsize(src_path),
                                    "modification_time": datetime.fromtimestamp(
                                        os.path.getmtime(src_path)
                                    ).isoformat(),
                                }
                            )
                        else:
                            curr_metadata.append(metadata_dict[rel_path])
                    else:
                        link_name = linkto
                        curr_metadata.append(metadata_dict[rel_path])
                else:
                    if os.path.islink(src_path):
                        linkto = os.readlink(src_path)
                        if os.path.exists(dst_path):
                            os.remove(
                                dst_path
                            )  # Remove existing destination if it exists
                        os.symlink(linkto, dst_path)
                        link_name = linkto
                        file_type = "FL"
                    else:
                        shutil.copy2(src_path, dst_path)
                        file_type = "F"
                        file_size = os.path.getsize(dst_path)

                    curr_metadata.append(
                        {
                            "relative_path": rel_path,
                            "destination_path": dst_path,
                            "file_type": file_type,
                            "link_to": link_name,
                            "file_size": (
                                os.path.getsize(src_path) if file_type == "F" else None
                            ),
                            "modification_time": datetime.fromtimestamp(
                                os.path.getmtime(src_path)
                            ).isoformat(),
                        }
                    )

    with open(metadata_file_path, "w") as metadata_file:
        json.dump(curr_metadata, metadata_file, indent=4)

    snapshot_id_file_map.append(
        {"snapshot_id": snapshot_id, "file_path": metadata_file_path}
    )
    update_snapshot_id_file(snapshot_id_map_file, snapshot_id_file_map)

    backup_info = {
        "backup_dir": source_dir,
        "version": version,
        "backup_id": snapshot_id,
        "timestamp": datetime.now().isoformat(),
    }

    append_and_sort_records(metadata_backup_list_file, backup_info)
    print("Backup successfully completed, Sanpshot ID: ", snapshot_id)


def do_restore(target_dir, repopath, snapshotid):
    backup_dir = os.path.join(repopath, BACKUP_DIR_NAME)
    metadata_dir = os.path.join(repopath, METADATA_DIR_NAME)
    snapshot_id_map_file = os.path.join(metadata_dir, SNAPSHOT_ID_FILE_MAP)
    metadata_backup_list_file = os.path.join(metadata_dir, BACKUP_LIST)

    if os.path.exists(target_dir):
        shutil.rmtree(target_dir)
    os.makedirs(target_dir)

    metadata_file_name = get_filename_for_snapshot_id(snapshot_id_map_file, snapshotid)
    with open(metadata_file_name, "r") as metadata_file:
        metadata_file_data = json.load(metadata_file)
    for sublist in metadata_file_data:
        rel_path = sublist["relative_path"]
        src_path = sublist["destination_path"]
        file_type = sublist["file_type"]
        link_name = sublist["link_to"]
        dst_path = os.path.join(target_dir, rel_path)
        if file_type == "D":
            os.makedirs(dst_path)
        elif file_type == "DL":
            os.symlink(link_name, dst_path)

    for sublist in metadata_file_data:
        rel_path = sublist["relative_path"]
        src_path = sublist["destination_path"]
        file_type = sublist["file_type"]
        link_name = sublist["link_to"]
        dst_path = os.path.join(target_dir, rel_path)
        if file_type == "F":
            shutil.copy2(src_path, dst_path)
        elif file_type == "FL":
            os.symlink(link_name, dst_path)


def read_data(file_path):
    """Read JSON data from the file."""
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        return []
    with open(file_path, "r") as file:
        try:
            data = json.load(file)
        except json.JSONDecodeError as e:
            print(f"Error reading JSON file: {e}")
            return []
    return data


def write_data(file_path, data):
    """Write JSON data to the file."""
    with open(file_path, "w") as file:
        try:
            json.dump(data, file, indent=4)
        except (TypeError, IOError) as e:
            print(f"Error writing JSON file: {e}")


def append_and_sort_records(file_path, new_record):
    """Append a new record to the file and sort records by timestamp."""
    # Read existing data
    data = read_data(file_path)

    # Convert single object to list if necessary
    if isinstance(data, dict):
        data = [data]

    if not isinstance(data, list):
        print("Invalid data format in backup file.")
        return

    # Append new record
    data.append(new_record)

    # Write sorted data back to file
    write_data(file_path, data)


def do_init(repopath):
    if os.path.exists(repopath):
        shutil.rmtree(repopath)
    os.makedirs(repopath)


def do_list(repopath):
    metadata_dir = os.path.join(repopath, METADATA_DIR_NAME)
    metadata_backup_list_file = os.path.join(metadata_dir, BACKUP_LIST)
    file_path = metadata_backup_list_file

    # Check if the file exists and has content
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        print("No backup records found.")
        return

    # Read the existing data from the file
    with open(file_path, "r") as file:
        try:
            data = json.load(file)
        except json.JSONDecodeError as e:
            print(f"Error reading JSON file: {e}")
            return

    # Handle both single object and list of objects
    if isinstance(data, dict):
        data = [data]  # Convert a single object to a list

    if not isinstance(data, list):
        print("Invalid data format in backup file.")
        return

    # Print each record
    for record in data:
        if not isinstance(record, dict):
            print("Invalid record format.")
            continue

        # Extract values and convert timestamp to human-readable format
        backup_dir = record.get("backup_dir", "N/A")
        version = record.get("version", "N/A")
        backup_id = record.get("backup_id", "N/A")
        timestamp_str = record.get("timestamp", "N/A")

        # Convert timestamp string back to datetime object if needed
        try:
            timestamp = datetime.fromisoformat(timestamp_str)
            timestamp_formatted = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            timestamp_formatted = timestamp_str

        # Print the record
        print(
            f"Backup Dir: {backup_dir}, Version: {version}, Backup ID: {backup_id}, Timestamp: {timestamp_formatted}"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Perform backup, restore, or list operations."
    )
    parser.add_argument(
        "-c",
        "--command",
        choices=["init", "backup", "restore", "list"],
        required=True,
        help="Specify the command type: backup, restore, or list.",
    )

    # Common argument
    parser.add_argument(
        "-r",
        "--repopath",
        required=True,
        help="Repository directory path to keep backup",
    )

    # Arguments for backup command
    parser.add_argument(
        "-s", "--source", help="Source directory path (required for backup)"
    )

    # Arguments for restore command
    parser.add_argument(
        "-d",
        "--destination",
        help="Target directory path for restore (required for restore)",
    )
    parser.add_argument("-i", "--snapshotid", help="Snapshot ID (required for restore)")

    args = parser.parse_args()

    if args.command == "init":
        do_init(args.repopath)
    elif args.command == "backup":
        do_backup(args.source, args.repopath)
    elif args.command == "restore":
        do_restore(args.destination, args.repopath, args.snapshotid)
    elif args.command == "list":
        do_list(args.repopath)
    else:
        print_usage_error("Invalid command. Please use 'backup', 'restore', or 'list'.")


if __name__ == "__main__":
    main()
