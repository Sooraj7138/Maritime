import requests
import PyPDF2
from docx import Document
import csv
import re
from datetime import datetime
import ollama
import os

# def ask(prompt):
#     payload = {
#         "model": "llama3.2:3b",
#         "messages": [
#             {"role": "user", "content": prompt}
#         ],
#         "stream": False
#     }
#     response = requests.post(API_URL, json=payload)
#     response.raise_for_status()
#     data = response.json()
#     return data["message"]["content"]

path = "./Maritime Regulations & Rules"
os.makedirs("./MaritimeR&R_OP", exist_ok=True)

def ask(prompt):
    response = ollama.chat(
        model='llama3.2:latest',
        messages=[
            {'role': 'user', 'content': prompt}
        ]
    )
    return response['message']['content']

def read_docx(path):
    doc = Document(path)
    text = []
    for para in doc.paragraphs:
        if para.text.strip():
            text.append(para.text)
    return "\n".join(text)

def read_pdf(path):
    with open(path, 'rb') as file:
        pdf_reader = PyPDF2.PdfReader(file)
        text = ''
        for page in pdf_reader.pages:
            text += page.extract_text() + '\n'
    return text 

def prompter():
    files = os.listdir(path)
    for file in files:
        if not file.endswith('.pdf'):
            continue
        # print("files : ",os.listdir(path))
        print(f"|--------------------Processing file : {file}--------------------|")
        doc_content = read_pdf(f"./{path}/{file}")
        # print("Doc :",doc_content[:500])  # Debug: print first 500 characters of the document

        matches = []
        retries = 0
        max_retries = 3
        while not matches and retries < max_retries:
            if retries > 0:
                print(f"⚠️ No Q&A pairs found for {file}. Retrying ({retries}/{max_retries})")
            
            prompt = f"""
            **You are a helpful assistant. Carefully read the following document and generate exactly 20-25 unique
            question-and-answer exchanges based strictly on its content.**

            **Requirements:**
            - Each question must be simple, clear, direct, and context-aware (reflecting the incident or topic in the document).
            - Each answer must be detailed in 2 lines, concise but informative.
            - Questions should include a mix of summarize, describe, and analytical styles.
            - Do NOT repeat or rephrase exchanges; all 20-25 must be distinct.
            - Do NOT invent information outside the document.
            - Output must strictly follow the format below, with no extra commentary or numbering.

            Document:
            {doc_content}

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
            output_file = file.replace(" ", "_").replace(".pdf","")
            with open(f"./MaritimeR&R_OP/Output_{output_file}.csv", "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["instruction", "input", "output"])
                for i, (q, a) in enumerate(matches, start=1):
                    writer.writerow([q.strip(),"", a.strip()])
            print(f"✅ Q&A pairs saved to Output_{output_file}.csv")
        else:
            print(f"❌ Failed to generate Q&A pairs for {file} after {max_retries} retries. Skipping.")

if __name__ == "__main__":
    prompter()

