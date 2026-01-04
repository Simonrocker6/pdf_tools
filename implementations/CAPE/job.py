import json
from openai import OpenAI
import csv
from prompt import BASE_PROMPT


RETRY_TIMES = 5

# 火山模型
huoshan_client = OpenAI(
    api_key="***",
    base_url="https://ark.cn-beijing.volces.com/api/v3",
)
HUOSHAN_K2_MODEL = "kimi-k2-250711"
HUOSHAN_DEEPSEEK_MODEL = "deepseek-v3-1-terminus"


# 加载 JSON 文件
def load_data(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data


def call_LLM(prompt, model=HUOSHAN_DEEPSEEK_MODEL):
    response = huoshan_client.chat.completions.create(
        model=model,
        temperature=0.1,
        messages=[
            {"role": "user", "content": prompt}
        ]
    )
    return response.choices[0].message.content

def write_result(result):
    with open('result.csv', 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["profile", "conversation"] + [f"response_{i}" for i in range(RETRY_TIMES)])
        for item in result:
            profile_json = json.dumps(item["profile"], ensure_ascii=False)
            conversation_json = json.dumps(item["conversation"], ensure_ascii=False)
            writer.writerow([profile_json, conversation_json] + item["responses"])


if __name__ == '__main__':



    

    profiles = load_data('data_profiles.json')
    conversations = load_data('data_conversations.json')

    result = []
    # 用笛卡尔乘积组合所有可能的对话
    for profile in profiles:
        for conversation in conversations:
            # 合并 profile 和 conversation 数据
            combined_data = {**profile, **conversation}
            print(combined_data)
            # 生成 prompt
            prompt = BASE_PROMPT + json.dumps(combined_data, ensure_ascii=False)
            # print(prompt)
            responses = []
            # 调用 LLM
            for i in range(RETRY_TIMES):
                response = call_LLM(prompt)
                responses.append(response)
            result.append({
                "profile": profile,
                "conversation": conversation,
                "responses": responses
            })
            
    
    write_result(result)
