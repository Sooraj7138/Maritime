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
        model="moonshotai/kimi-k2-instruct-0905",
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
    file_name_path = f"./Maritime Regulations & Rules/{file.split('.')[0]}/{file}"
    matches = []
    retries = 0
    max_retries = 6
  
    while not matches and retries < max_retries:
        if retries > 0:
            print(f"⚠️ No Q&A pairs found for {file}. Retrying ({retries}/{max_retries})")
        
        # prompt = f"""
        # **You are a maritime domain expert specializing in regulatory compliance. Carefully read the following document and generate exactly 20 unique, behavioral, scenario-based, context-specific question-and-answer exchanges based strictly on its English content. If there is low content or pages in the document, generate only as many as possible, like 5-10 unique exchanges.**

        # **Requirements:**
        # - Do NOT generate factual exchanges; focus solely on compliance audit scenarios.
        # - Each exchange must be behavioral, scenario-based, and context-specific to the document's regulations on pollution prevention, vessel equipment, port facilities, and compliance actions.
        # - Go through the entire English content of the document thoroughly.
        # - Do NOT mention the document name, location, relative path, or any metadata in the exchanges.
        # - The Document name will be {file.split(".")[0]} for reference only.
        # - Each problem (question) must be a compliance scenario: simple, clear, direct, and context-aware (e.g., 'For a 1200 GT inland vessel without oily mixture treatment, what compliance action should the surveyor take?').
        # - For each exchange, provide a step-by-step chain of thought reasoning in a <think> tag, leading to a **Final Answer**.
        # - The final answer inside the box must be concise, 1-2 lines, informative, behavioral, and scenario-based (e.g., 'Issue a notice under Rule 9(ii) and suspend operations until remedied.').
        # - Do NOT repeat or rephrase exchanges; all must be distinct. Do NOT invent information outside the document.
        # - Output must strictly follow the format below, with no extra commentary or numbering.
        # - Analyze the entire English content clearly and thoroughly before generating exchanges.**
        # - Don't miss the main conditions like 20 Exchanges, Thorough Analysis, Strict Format, No Repetition, Behavioral, Scenario-based, Context Awareness, Document Focus.
        # - Do not extract or use other language information; just use English contents only.
       
        # Document:
        # {file_name_path}

        # **Output format (strictly follow):**
        # Problem: [The scenario-based question here]
        # Generated Solution: <think>[Step-by-step chain of thought reasoning leading to the answer]  </think> **Final Answer**: [The concise, behavioral, scenario-based final answer here]
        # Expected Output: [The final compliance recommendation in 1-2 lines]
        # """

        prompt = f"""
        You are a maritime domain expert specializing in regulatory compliance. Carefully read the following document and generate exactly 20 unique, behavioral, scenario-based, context-specific question-and-answer exchanges based strictly on its English content. If there is low content or pages in the document, generate only as many as possible (like 5–10 unique exchanges).

        Requirements:
        - Do NOT generate factual exchanges; focus solely on compliance audit scenarios.
        - Each exchange must be behavioral, scenario-based, and context-specific to the document’s regulations on pollution prevention, vessel equipment, port facilities, and compliance actions.
        - Go through the entire English content thoroughly; use only English content.
        - Do NOT mention the document name, location, relative path, or any metadata in the exchanges.
        - The document basename is {file.split(".")[0]} (for metadata only, not to be included in exchanges).
        - Each problem must be a simple, clear, direct compliance scenario (e.g., "For a 1200 GT inland vessel without oily mixture treatment, what compliance action should the surveyor take?").
        - For each exchange, provide scenario-based question, leading to a concise, behavioral, scenario-based final answer here.
        - The final answer must be concise (1–2 lines), informative, behavioral, and scenario-based (e.g., "Issue a notice under Rule 9(ii) and suspend operations until remedied.").
        - All exchanges must be distinct. Do NOT invent information outside the document.

        Document:
        {file_name_path}

        **Output format (strictly follow):**
        prompt: [Scenario-based compliance question] 
        solution: [The concise, behavioral, scenario-based final answer here]"
        """

        qa_output = ask(prompt)

        # Debug: print raw output
        # print("RAW OUTPUT:\n", qa_output)

        # Regex to match the new format
        pattern = re.compile(r"prompt:\s*(.*?)\s*solution:\s*(.*?)(?=\nprompt:|\Z)", re.DOTALL)
        matches = pattern.findall(qa_output)
        retries += 1
        
    if matches:
        print(f"✅ Q&A pairs extracted for {file}")
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
                with open(f"./LTS_OP/new1/{base}.csv", "w", newline="", encoding="utf-8") as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(["prompt", "solution"])
                    for prob, sol in all_matches:
                        writer.writerow([prob.strip(), sol.strip()])
                print(f"✅ Q&A pairs for {base} saved to {base}.csv\n")
            else:
                print(f"❌ No Q&A pairs for {base}.")

if __name__ == "__main__":
    # main_extractor("Maritime_ops.pdf")
    input_dir = "./Maritime Regulations & Rules"
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
