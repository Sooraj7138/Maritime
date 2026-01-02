import csv
import json
import os

converted = []

# for file in os.listdir("LTS_OP"):
#     if file.endswith(".csv"):
with open(f"./Test_CSV_Output/combined_csv.csv", "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        # Build user message
        if row["input"].strip():
            user_msg = f"{row['instruction']}\n{row['input']}"
        else:
            user_msg = row["instruction"]

        # Build assistant message
        assistant_msg = row["output"]

        # Wrap into FineTome-style format
        new_item = {
            "conversation": [
                {"role": "user", "content": user_msg},
                {"role": "assistant", "content": assistant_msg}
            ],
            "source": "custom-dataset",
            "score": 10
        }
        converted.append(new_item)

# Save converted dataset
with open("combined_dataset.json", "w", encoding="utf-8") as f:
    json.dump(converted, f, indent=2, ensure_ascii=False)
