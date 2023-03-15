# Datastore82*
from treelib import Tree, Node
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import dateutil.parser
import argparse
import datetime
import pytz
from pathlib import Path
import json
import os
import glob

arg_parser = argparse.ArgumentParser(description='Add more arguments after parsing.',add_help=False)
arg_parser.add_argument('MODE',  default='default',type=str, help='What commands to use')

args = arg_parser.parse_known_args()[0]

if args.MODE == 'push':
    arg_parser.add_argument('--repo') # Repo to save to 
    arg_parser.add_argument('--filepath') # File base path to extract from
    arg_parser.add_argument('--filename') # File in base path to extract

elif args.MODE == 'pull' :
    arg_parser.add_argument('--repo') # Repo to pull from
    arg_parser.add_argument('--filename') # File name in repo to pull
    arg_parser.add_argument('--filepath') # File base path to save to

arg_parser.add_argument(
            '-h', '--help',
            action='help', default=argparse.SUPPRESS,
            help=argparse._('show this help message and exit'))

args = arg_parser.parse_args()

global_mode = args.MODE

def main():
    gAuth = GoogleAuth()
    #authenticate_user(gAuth, "client_credentials.txt")
    drive = GoogleDrive(gAuth)

    tree = FileStructure()

    with open("defaults.json") as json_file:
        defaults = json.load(json_file)

    if args.MODE == "push":
        # Pushing to Drive

        if args.filepath is None:
            filepath = defaults['default_extract_path']
        else:
            filepath = args.filepath

        # Check whether the user has specified a file name for storage
        if args.filename is None:
            filename = check_latest_in_dir(filepath)
        else:
            filename = args.filename

        filepath = os.path.join(filepath, filename)

        extract_path = Path(filepath)
        if not extract_path.exists():
            print("The target filepath doesn't exist, or has been spelt incorrectly. Please try again.")
            raise SystemExit(1)

        # Get the repo to save
        if args.repo is None:
            repo = defaults['default_repo']
        else:
            repo = args.repo

        # Check to see if the repo exists, return its id or create one
        id = tree.check_root_or_create(drive, repo, True)

        file = commit_file(drive, id, str(extract_path), filename)
        print(f"Push Success. {file['title']} has been uploaded to {repo}.")

    elif args.MODE == "pull":
        # Pulling file from Drive

        if args.repo is None:
            repo = defaults['default_repo']
        else:
            repo = args.repo

        # Check repo exists and get its id
        id = tree.check_root_or_create(drive, repo, False)
        if id is None:
            print("The repo does not exist, or has been spelt incorrectly. Please try again.")
            raise SystemExit(1)
        
        #root_folder_id = tree.get_folder_id(drive, id, repo)
        tree.tree.create_node(repo, id)
        tree.populate_tree_recursively(drive, id)
        
        if args.filename is None:
            print("Filename not specified. Retrieving last saved file...")
            file = tree.get_latest_file_in_folder(drive, id)
            if file is not None:
                print(f"Latest file found: {file.tag}. Created: {file.data.created}")
        else:
            filename = args.filename
            file = tree.get_file_by_name(drive, id, filename)

        if args.filepath is None:
            filepath = defaults['default_deposit_path']
        else:
            filepath = args.filepath

        if not os.path.exists(filepath):
            print("Default pull directory doesn't exist! Creating it now...")
            os.makedirs(filepath)

        if file is None:
            print("This repo is empty!")
            raise SystemExit(1)

        filepath = os.path.join(filepath, file.tag)

        retrieve_file(drive, file.identifier, filepath)
        print(f"Pulled successfully from {repo}. {file.tag} has been retrieved and saved as {filepath}.")

    else:
        print("Not a valid MODE. Please try again.")

class NodeData:
    def __init__(self, id, type, created):
        self.id = id
        self.type = type
        self.created = created

class FileStructure:
    def __init__(self):
        self.tree = Tree()

    def get_folder_id(self, drive: GoogleDrive, root: str, root_id: str):
        file_ls = self.get_children(drive, root_id)
        for file in file_ls:
            if (file['title'] == root):
                return file['id']

    def get_children(self, drive: GoogleDrive, root_id: str):
        str = "\'" + root_id + "\'" + " in parents and trashed=false"
        file_ls = drive.ListFile({'q': str}).GetList()
        return file_ls
    
    def add_children_to_tree(self, drive: GoogleDrive, file_ls: list, parent_id: str):
        for file in file_ls:
            if file['mimeType'] == "application/vnd.google-apps.folder":
                file_type = 'folder'
            else:
                file_type = 'file'
            created = dateutil.parser.isoparse(file['createdDate'])
            self.tree.create_node(file['title'], file['id'], data = NodeData(file['id'], file_type, created), parent=parent_id)

    def populate_tree_recursively(self, drive: GoogleDrive, parent_id: str):
        children = self.get_children(drive, parent_id)
        self.add_children_to_tree(drive, children, parent_id)
        if(len(children) > 0):
            for child in children:
                self.populate_tree_recursively(drive, child['id'])

    def generate_file_list(self, drive: GoogleDrive):
        file_ls = drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()
        return file_ls
    
    def check_root_or_create(self, drive: GoogleDrive, repo_name: str, create_if_not: bool):
        file_ls = self.generate_file_list(drive)
        for file in file_ls:
            if file['title'] == repo_name:
                return file['id']

        if create_if_not:
            print("A repo with this name does not exist. Creating a new repo...")
            folder = self.create_folder(drive, repo_name)
            return folder['id']
        else:
            return None

    def get_latest_file_in_folder(self, drive: GoogleDrive, parent_id: str):
        leaves = self.tree.leaves(parent_id)

        leaves_time = [leaf.data.created for leaf in leaves if leaf.data is not None]

        now = datetime.datetime.now(pytz.utc)
        d_t = [dt for dt in leaves_time if dt < now]

        if len(d_t) == 0:
            return None

        last_mod_idx = max(range(len(d_t)), key=d_t.__getitem__)
        return leaves[last_mod_idx]
    
    def get_file_by_name(self, drive: GoogleDrive, parent_id: str, name: str):
        leaves = self.tree.leaves(parent_id)
        for leaf in leaves:
            if leaf.tag == name:
                return leaf
        return None

    def create_folder(self, drive: GoogleDrive, folder_name: str, parent_id: str = None):
        metadata = {"title": folder_name, "mimeType": "application/vnd.google-apps.folder"}
        folder = drive.CreateFile(metadata)
        folder.Upload()

        return folder

def check_latest_in_dir(filepath):
    path = os.path.join(filepath, '*')
    list_of_files = glob.glob(path)
    if len(list_of_files) == 0:
        print("This directory is empty or doesn't exist!")
        raise SystemExit(1)
    latest_file = max(list_of_files, key=os.path.getctime)
    return os.path.basename(latest_file)

def authenticate_user(gAuth: GoogleAuth, credentials: str):
    gAuth.LoadCredentialsFile(credentials)

    if gAuth.credentials is None:
        # Authenticate if cred file doesn't exist
        gAuth.LocalWebserverAuth()
    elif gAuth.access_token_expired:
        # Refresh if tokens have expired
        gAuth.Refresh()
    else:
        # Authenticate with saved cred file
        gAuth.Authorize()

    gAuth.SaveCredentialsFile(credentials)

    return gAuth

def commit_file(drive: GoogleDrive, folder_id: int, file_path: str, file_name: str):
    file = drive.CreateFile({"parents": [{"kind": "drive#fileLink", "id": folder_id}]})
    file.SetContentFile(file_path)
    file['title'] = file_name
    file.Upload()

    return file

def retrieve_file(drive: GoogleDrive, file_id: str, name: str):
    file = drive.CreateFile({'id': file_id})
    file.GetContentFile(name)



def generate_file_tree(drive: GoogleDrive, file_tree: FileStructure, root: str, root_id: str):
    root_id = file_tree.get_folder_id(drive, root, root_id)

    file_tree.tree.create_node(root, root_id)
    file_tree.populate_tree_recursively(drive, root_id)
    file_tree.tree.show()

main()








