from docx import Document
from PyPDF2 import PdfReader
import os

os.makedirs("./TestOutput", exist_ok=True)

def split_paragraphs_to_chunks(docx_path, output_txt_path, chunk_size=500):
    """
    Reads a DOCX file, splits its paragraphs into chunks, and saves them to a text file.
    
    :param docx_path: Path to the input DOCX file
    :param output_txt_path: Path to the output TXT file
    :param chunk_size: Maximum number of characters per chunk
    """
    # Load the document
    doc = Document(docx_path)
    
    chunks = []
    
    # Iterate through paragraphs
    for para in doc.paragraphs:
        text = para.text.strip()
        if not text:
            continue  # skip empty paragraphs
        
        # Split into chunks
        for i in range(0, len(text), chunk_size):
            chunk = text[i:i+chunk_size]
            chunks.append(chunk)
    
    # Save chunks into a text file
    with open(output_txt_path, "w", encoding="utf-8") as f:
        for idx, chunk in enumerate(chunks, 1):
            # f.write(f"--- Chunk {idx} ---\n")
            f.write(chunk + "\n\n")

    print(f"Saved {len(chunks)} chunks to {output_txt_path}")


def split_pdf_to_chunks(pdf_path, output_txt_path, chunk_size=2048):
    """
    Reads a PDF file, extracts text, splits it into chunks, and saves them to a text file.
    
    :param pdf_path: Path to the input PDF file
    :param output_txt_path: Path to the output TXT file
    :param chunk_size: Maximum number of characters per chunk
    """
    # Load the PDF
    reader = PdfReader(pdf_path)
    
    text = ""
    # Extract text from all pages
    for page in reader.pages:
        text += page.extract_text() + "\n"
    
    # Remove newlines to make it one continuous text
    text = text.replace('\n', ' ')
    
    chunks = []
    
    # Split into chunks
    for i in range(0, len(text), chunk_size):
        chunk = text[i:i+chunk_size]
        chunks.append(chunk)
    
    # Save chunks into a text file
    with open(output_txt_path, "w", encoding="utf-8") as f:
        for idx, chunk in enumerate(chunks, 1):
            # f.write(f"--- Chunk {idx} ---\n")
            f.write(chunk + "\n")

    print(f"Saved {len(chunks)} chunks to {output_txt_path}")

def combine_text_files(output_folder, combined_file_path):
    """
    Combines all .txt files in the output folder into a single text file, preserving chunk separations.
    
    :param output_folder: Path to the folder containing the text files
    :param combined_file_path: Path to the output combined text file
    """
    combined_text = ""
    for file_name in os.listdir(output_folder):
        if file_name.endswith(".txt"): 
            file_path = os.path.join(output_folder, file_name)
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
                combined_text += content + "\n\n"
    
    with open(combined_file_path, "w", encoding="utf-8") as f:
        f.write(combined_text)
    
    print(f"Combined all text files into {combined_file_path}")

# Example usage:
# split_paragraphs_to_chunks("MARITIME_INCIDENTS_3.docx", "output.txt", chunk_size=500)
for file in os.listdir("./Test"):
    print("file :",file)
    if file.endswith(".pdf"):
        split_pdf_to_chunks(f"./Test/{file}", f"./TestOutput/output_{file.split('.')[0]}.txt")

# Combine all output text files into one
combine_text_files("./TestOutput", "./TestOutput/combined_output.txt")