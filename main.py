import requests
from unstructured.partition.pdf import partition_pdf
from docx import Document
import csv
import re
from datetime import datetime
import ollama
import os
from groq import Groq
from dotenv import load_dotenv
import pandas as pd
import fitz  # PyMuPDF
from collections import defaultdict

load_dotenv()
os.makedirs("./MaritimeR&R_OP", exist_ok=True)
os.makedirs("./Test_CSV_Output", exist_ok=True)
os.makedirs("./LTS_OP", exist_ok=True)

def ask(prompt):
    client = Groq()
    completion = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {
                "role": "user",
                "content": prompt
            }
        ]
    )
    return completion.choices[0].message.content

def read_docx(path):
    doc = Document(path)
    text = []
    for para in doc.paragraphs:
        if para.text.strip():
            text.append(para.text)
    return "\n".join(text)

def read_pdf(path):
    elements = partition_pdf(path)
    text = "\n".join([str(el) for el in elements])
    return text 

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

def main_extractor(file):
    print(f"|====================Processing file : {file}")                
    # doc_content = read_pdf(f"./{path}/{file}")
    file_name_path = f"./Split_PDFs/{file.split('.')[0]}/{file}"
    matches = []
    retries = 0
    max_retries = 6
  
    while not matches and retries < max_retries:
        if retries > 0:
            print(f"⚠️ No Q&A pairs found for {file}. Retrying ({retries}/{max_retries})")
        
        prompt = f"""
        **You are an maritime domain expert. Carefully read the following document and generate exactly 20 unique, behavioral, scenario based, 
        context-specific question-and-answer exchanges based strictly on its content. If there is low content or pages in document 
        just generate only as many as possible like 5-10 unique exchanges**

        **Requirements:**
        - Do Not generate factual exchanges even if the document has mentioned them.
        - Each Q&A pair must be behavioral, scenario based, context-specific to the document.
        - Go through the entire document thoroughly.
        - Do Not Mention the document name, location, relative path, or any metadata in the Q&A pairs.
        - The Document name will be {file.split(".")[0]} for reference only.
        - Each question must be mentioned simple, clear, direct, and with context-aware(reflecting the incident or topic in the document) on what context the question is about , 
            example:- What does the agreement concerning manned lightships tells about? , 
                        What does the article 2 says in the agrrement concerning manned lightships?
        - Each answer must be detailed in 1-2 lines, concise but informative not in factual aspect but behavioral and scenario-based.
        - Do NOT repeat or rephrase exchanges; all 20 must be distinct. Do NOT invent information outside the document.
        - Output must strictly follow the format below, with no extra commentary or numbering.
        - Analyse the entire document clear and thoroughly before generating Q&A pairs.**
        - Dont Miss the Main Conditions like 20 Q&A Pairs, Thorough Analysis, Strict Format, No Repetition, Behavioural , Scenario based, Context Awareness, Document Focus.

        Document:
        {file_name_path}

        **Output format (strictly follow):**
        Q: ...
        A: ...
        """

        qa_output = ask(prompt)

        # Debug: print raw output
        # print("RAW OUTPUT:\n", qa_output)

        # Regex that tolerates numbering before and after Q/A:
        pattern = re.compile(r"(?:\d+\.\s*)?Q\d*:\s*(.*?)\s*A\d*:\s*(.*?)(?=\n(?:\d+\.\s*)?Q\d*:|\Z)", re.DOTALL)
        matches = pattern.findall(qa_output)
        retries += 1
        
    if matches:
        print(f"✅ Q&A pairs extracted for {file}\n")
        return matches
    else:
        print(f"❌ Failed to generate Q&A pairs for {file} after {max_retries} retries. Skipping.")
        return []

def prompter(path):
    base_to_matches = defaultdict(list)
    times = 0
    for file in os.listdir(path):
        if "_part_" in file:
            base = file.split("_part_")[0]
        else:
            base = file.split(".")[0]  # in case not split
        matches = main_extractor(file)
        base_to_matches[base].extend(matches)
    
    for base, all_matches in base_to_matches.items():
        if all_matches:
            with open(f"./LTS_OP/{base}.csv", "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["instruction", "input", "output"])
                for q, a in all_matches:
                    writer.writerow([q.strip(), "", a.strip()])
            print(f"✅ Q&A pairs for {base} saved to {base}.csv")
        else:
            print(f"❌ No Q&A pairs for {base}.")

if __name__ == "__main__":
    # main_extractor("Maritime_ops.pdf")
    input_dir = "./Long_term_stratigies"
    for file in os.listdir(input_dir):
        output_dir = f"./Split_PDFs/{file.split('.')[0]}"
        os.makedirs(output_dir, exist_ok=True)
        if os.listdir(output_dir):
            print(f"Skipping {file} as output directory already exists.")
        else:
            split_pdf(os.path.join(input_dir, file), output_dir)
        prompter(output_dir)
    
    # for file in os.listdir(input_dir):
    #     if file.endswith('.pdf'):
    #         input_path = os.path.join(input_dir, file)
    #         split_pdf(input_path, output_dir)
