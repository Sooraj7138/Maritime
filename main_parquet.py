import json
import os
import PyPDF2
import re
from unstructured.partition.auto import partition
import pandas as pd
from itertools import chain
from functools import partial
from transformers import AutoTokenizer
from datasets import load_dataset
from dotenv import load_dotenv
from transformers import AutoTokenizer

load_dotenv()

try:
    file_actual = "MPI"
    folder_path = "./pdfs"
    output_dir = f"./{file_actual}"
    os.makedirs(output_dir, exist_ok=True)

    def read_pdfs_from_folder(folder_path):
        pdf_texts = {}
        for filename in os.listdir(folder_path):
            if filename.lower().endswith(".pdf"):
                filename = f"{file_actual}.pdf"
                file_path = os.path.join(folder_path, filename)
                try:
                    # with open(file_path, "rb") as f:
                    #     reader = PyPDF2.PdfReader(f)
                    #     text = ""
                    #     for page in reader.pages:
                    #         text += page.extract_text() or ""
                    #     pdf_texts[filename] = text
                    elements = partition(filename=file_path)
                    outs = "\n\n".join([str(el) for el in elements])
                    pdf_texts[filename] = outs
                except Exception as e:
                    print(f"Error reading {filename}: {e}")
        return pdf_texts

    # # Run the function
    # pdf_contents = read_pdfs_from_folder(folder_path)

    # # Print results
    # for name, content in pdf_contents.items():
    #     print(f"\n--- {name} ---\n")
    #     print(content[:])
    
    def runner():
        # ---------- CLEANING ----------
        def clean_text(text):
            text = text.replace("\x00", "")    # remove null chars
            text = re.sub(r'\s+', ' ', text)   # normalize whitespace
            return text.strip()

        def add_newlines(text):
            text = re.sub(r'(?<!\d)\.\s+', '.\n', text)
            text = re.sub(r'\?\s+', '?\n', text)
            text = re.sub(r'!\s+', '!\n', text)
            text = re.sub(
                r'(?<=\s)(\d+)\.\s+(?=[A-Z])',
                r'\n\1. ',
                text
            )
            text = text.replace("• ", "\n• ")

            return text
            
        # ---------- CHUNKING ----------
        def chunk_text_by_words(text, words_per_chunk=1500):
            words = text.split()
            chunks = []

            for i in range(0, len(words), words_per_chunk):
                chunk_words = words[i:i + words_per_chunk]
                chunk = " ".join(chunk_words)
                chunks.append(chunk)

            return chunks

        # ---------- RUN PIPELINE ----------
        pdf_contents = read_pdfs_from_folder(folder_path)

        chunk_id = 0

        for pdf_name, content in pdf_contents.items():
            if not content.strip():
                continue

            cleaned_text = clean_text(content)
            chunks = chunk_text_by_words(cleaned_text, words_per_chunk=1500)

            for chunk in chunks:
                chunk = add_newlines(chunk)
                wrapped_chunk = f"<doc_start>\n{chunk}\n<doc_end>\n"

                with open(
                    os.path.join(output_dir, f"chunk_{chunk_id:06d}.txt"),
                    "w",
                    encoding="utf-8"
                ) as f:
                    f.write(wrapped_chunk)

                chunk_id += 1

        print(f"✅ Dataset creation completed. Total chunks: {chunk_id}")
        create_jsonl()
        
    def create_jsonl():
        input_dir = f"./{file_actual}"
        output_file = f"./JSONL/{file_actual}.jsonl"
        os.makedirs("./JSONL", exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as out_f:
            for filename in sorted(os.listdir(input_dir)):
                if filename.endswith(".txt"):
                    file_path = os.path.join(input_dir, filename)

                    with open(file_path, "r", encoding="utf-8") as f:
                        text = f.read().strip()

                    # Optional: remove doc markers if you want
                    text = text.replace("<doc_start>", "").replace("<doc_end>", "").strip()

                    record = {
                        "text": text
                    }

                    out_f.write(json.dumps(record, ensure_ascii=False) + "\n")

        print("✅ JSONL file created:", output_file)
        create_parquet()

    def create_parquet():
        input_file = f"./JSONL/{file_actual}.jsonl"
        output_file = f"./Parquet/{file_actual}.parquet"
        os.makedirs("./Parquet", exist_ok=True)
        df = pd.read_json(input_file, lines=True)
        df.to_parquet(output_file, engine="pyarrow", index=False)
        print("✅ Parquet file created:", output_file)

    
    def tokenizer(parquet_file, model_id, chunk_length=2048, hf_token=None):
        """
        Tokenize and chunk a Parquet dataset for LLM pretraining.
        
        Returns:
            lm_dataset: tokenized & chunked dataset
        """
        dataset = load_dataset(
            "parquet",
            data_files=parquet_file,
            split="train"
        )

        tokenizer = AutoTokenizer.from_pretrained(
            model_id,
            token=hf_token
        )
        tokenizer.pad_token = tokenizer.eos_token

        remainder = {
            "input_ids": [],
            "attention_mask": []
        }

        def chunk(sample):
            nonlocal remainder
            concatenated = {k: list(chain(*sample[k])) for k in sample.keys()}
            concatenated = {k: remainder[k] + concatenated[k] for k in concatenated.keys()}

            total_length = len(concatenated["input_ids"])
            if total_length < chunk_length:
                return {}
            usable_length = (total_length // chunk_length) * chunk_length
            result = {
                k: [concatenated[k][i:i + chunk_length] for i in range(0, usable_length, chunk_length)]
                for k in concatenated.keys()
            }
            remainder = {k: concatenated[k][usable_length:] for k in concatenated.keys()}

            result["labels"] = result["input_ids"].copy()

            return result

        tokenized = dataset.map(
            lambda x: tokenizer(x["text"]),
            batched=True,
            remove_columns=["text"]
        )

        lm_dataset = tokenized.map(
            chunk,
            batched=True
        )

        return lm_dataset

    if __name__ == "__main__":
        file_path = "./Parquet/Maritime_incidents_new.parquet"
        model_id = "mistralai/Mistral-7B-v0.1"
        hf_token = os.getenv("HF_TOKEN")
        lm_dataset = tokenizer(file_path, model_id, chunk_length=2048, hf_token=hf_token)
        # print("Total training samples:",lm_dataset[0].get("attention_mask"))
        # print("Total training samples:",lm_dataset.features["input_ids"]," ",lm_dataset.features["attention_mask"]," ",lm_dataset.features["labels"])
        tok = AutoTokenizer.from_pretrained(model_id)
        print(tok.decode(lm_dataset[0]["input_ids"], skip_special_tokens=False))

except Exception as e:
    print(f"An error occurred: {e}")
