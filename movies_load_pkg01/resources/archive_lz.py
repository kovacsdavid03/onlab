import os
import shutil
from datetime import datetime

def archive_lz(source_folder, archive_folder):
    try:
        if not os.path.exists(source_folder):
            raise FileNotFoundError(f"Source folder '{source_folder}' does not exist.")

        os.makedirs(archive_folder, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_filename = f"movies_load_pkg01_{timestamp}.zip"
        zip_filepath = os.path.join(archive_folder, zip_filename)

        shutil.make_archive(base_name=os.path.join(archive_folder, f"movies_load_pkg01_{timestamp}"),
                            format='zip',
                            root_dir=source_folder)

        print(f"Folder '{source_folder}' zipped successfully as '{zip_filename}'.")
        for item in os.listdir(source_folder):
            item_path = os.path.join(source_folder, item)
            if os.path.isfile(item_path) or os.path.islink(item_path):
                os.unlink(item_path) 
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path) 

        print(f"Contents of '{source_folder}' deleted successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")