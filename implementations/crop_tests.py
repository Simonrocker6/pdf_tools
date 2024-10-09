print('Find the pdfs ..')

from pypdf import PdfReader, PdfWriter
import os

cur_dir = os.path.dirname(os.path.abspath(__file__))
print("Current directory: ")
print(cur_dir)

for file_name in os.listdir(cur_dir):
    if file_name.endswith('.pdf'):
        absolute_file = os.path.join(cur_dir, file_name)
        print(f"Processing: {absolute_file}") 

        reader = PdfReader(absolute_file)
        writer = PdfWriter()

        page = reader.pages[0]
        # Modify the mediabox dimensions
        mediabox = page.mediabox

        mediabox.upper_right = (
            mediabox.right,
            mediabox.top * 8  / 9,
        )

        writer.add_page(page)
        output_file_name = f"{file_name}-cropped.pdf"
        output_file_path = os.path.join(cur_dir, output_file_name)

        with open(output_file_path, "wb") as fp: 
            writer.write(fp)