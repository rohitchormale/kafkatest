import json
import uuid


ranges = [1, 10, 100, 1000, 2000, 3000, 4000, 5000, 10000]
file_name_prefix = "foo"
file_name_postfix_map = {1: "1", 10: "10", 100: "100", 1000: "1K", 2000: "2K", 3000: "3K", 4000: "4K", 5000: "5K", 10000: "10K"}
file_extension = ".json"
data = {}

for i in ranges:
    data = {}
    for j in range(i):
        temp = str(uuid.uuid4())
        data[temp] = temp

    file_name = f"{file_name_prefix}{file_name_postfix_map[i] if i in file_name_postfix_map else str(i)}{file_extension}"
    with open(file_name, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

