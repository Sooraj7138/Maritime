from docx import Document
from PyPDF2 import PdfReader

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


def split_pdf_to_chunks(pdf_path, output_txt_path, chunk_size=500):
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
    
    chunks = []
    
    # Split into chunks
    for i in range(0, len(text), chunk_size):
        chunk = text[i:i+chunk_size]
        chunks.append(chunk)
    
    # Save chunks into a text file
    with open(output_txt_path, "w", encoding="utf-8") as f:
        for idx, chunk in enumerate(chunks, 1):
            # f.write(f"--- Chunk {idx} ---\n")
            f.write(chunk)

    print(f"Saved {len(chunks)} chunks to {output_txt_path}")


# Example usage:
# split_paragraphs_to_chunks("MARITIME_INCIDENTS_3.docx", "output.txt", chunk_size=500)
split_pdf_to_chunks("./Maritime Regulations & Rules/258375.pdf", "output_pdf.txt", chunk_size=500) 