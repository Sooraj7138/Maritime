import os
import fitz  # PyMuPDF

def split_pdf(input_path, output_dir, parts=5):
    doc = fitz.open(input_path)
    page_count = doc.page_count
    if page_count < parts:
        print(f"PDF {input_path} has only {page_count} pages, skipping split.")
        return
    
    chunk_size = page_count // parts
    remainder = page_count % parts
    
    base_name = os.path.splitext(os.path.basename(input_path))[0]
    
    start_page = 0
    for i in range(parts):
        extra = 1 if i < remainder else 0
        end_page = start_page + chunk_size + extra
        
        new_doc = fitz.open()
        new_doc.insert_pdf(doc, from_page=start_page, to_page=end_page-1)
        
        output_path = os.path.join(output_dir, f"{base_name}_part_{i+1}.pdf")
        new_doc.save(output_path)
        new_doc.close()
        
        print(f"Saved {output_path} with pages {start_page+1} to {end_page}")
        start_page = end_page

def main():
    input_dir = "./Long_term_stratigies"
    output_dir = "./Split_PDFs"
    os.makedirs(output_dir, exist_ok=True)
    
    for file in os.listdir(input_dir):
        if file.endswith('.pdf'):
            input_path = os.path.join(input_dir, file)
            split_pdf(input_path, output_dir)

if __name__ == "__main__":
    main()
