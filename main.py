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

load_dotenv()
path = "./Test"
os.makedirs("./MaritimeR&R_OP", exist_ok=True)
os.makedirs("./Test_CSV_Output", exist_ok=True)

def ask(prompt):
    client = Groq()
    completion = client.chat.completions.create(
        model="groq/compound",
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

def prompter():
    # files = "distress_safety_rc95 (1).pdf"
    
    # if not files.endswith('.pdf'):
    #     continue
    # print("files : ",os.listdir(path))
    times = 0
    for file in os.listdir(path):
        print(f"|====================Processing file : {file}")                
        # doc_content = read_pdf(f"./{path}/{file}")
        file_name_path = f"./Test/{file}"
        # doc_content = read_pdf(f"./Maritime Regulations & Rules/{files}")
        # print("Doc :",doc_content)  # Debug: print first 500 characters of the document
        matches = []
        retries = 0
        max_retries = 3
        while not matches and retries < max_retries:
            if retries > 0:
                print(f"⚠️ No Q&A pairs found for {file}. Retrying ({retries}/{max_retries})")
            
            prompt = f"""
            **You are a helpful assistant. Carefully read the following document and generate exactly 50-70 unique
            question-and-answer exchanges based strictly on its content. If there is low content or pages in document 
            just generate only as many as possible like 10-15 unique exchanges**

            **Requirements:**
            - Go through the entire document thoroughly.
            - Do Not Mention the document name, location, relative path, or any metadata in the Q&A pairs.
            - The Document name will be {file.split(".")[0]} for reference only.
            - Each question must be simple, clear, direct, and context-aware (reflecting the incident or topic in the document).
            - Each answer must be detailed in 1-2 lines, concise but informative.
            - Questions should include a mix of summarize, describe, and analytical styles.
            - Do NOT repeat or rephrase exchanges; all 50-70 must be distinct.
            - Do NOT invent information outside the document.
            - Output must strictly follow the format below, with no extra commentary or numbering.
            - Analyse the entire document clear and thoroughly before generating Q&A pairs.**
            - Dont Miss the Main Conditions like 50-70 Q&A Pairs, Thorough Analysis, Strict Format, No Repetition, Context Awareness, Document Focus.

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
            output_file = file.replace(" ", "_").replace(".pdf","")
            with open(f"./Test_CSV_Output/Output_{output_file}.csv", "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["instruction", "input", "output"])
                for i, (q, a) in enumerate(matches, start=1):
                    writer.writerow([q.strip(),"", a.strip()])
            print(f"✅ Q&A pairs saved to Output_{output_file}.csv")
        else:
            print(f"❌ Failed to generate Q&A pairs for {file} after {max_retries} retries. Skipping.")
        times += 1
        if times == 6:
            break

if __name__ == "__main__":
    prompter()

# from groq import Groq
# from dotenv import load_dotenv
# import os

# load_dotenv()

# client = Groq()
# completion = client.chat.completions.create(
#     model="llama-3.3-70b-versatile",
#     messages=[
#         {
#             "role": "user",
#             "content": "Explain why fast inference is critical for reasoning models"
#         }
#     ]
# )
# print(completion.choices[0].message.content)