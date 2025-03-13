import os
import json
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
import requests  # 改用 requests 库直接调用 API

@csrf_exempt
@require_http_methods(['POST'])
def chat_handler(request):
    try:
        # 解析 JSON 格式请求体（确保前端发送 Content-Type: application/json）
        data = json.loads(request.body)
        user_message = data.get('message', '')
        print("[Received Message]", user_message)

        # DeepSeek API 配置
        DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"
        DEEPSEEK_API_KEY = "sk-eb417ceae5974803a0400a8d3282de7f"  # 替换为有效 API KEY
        MODEL_NAME = "deepseek-chat"  # 根据实际模型名称调整

        # 构造请求头
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
            "Accept": "application/json"
        }

        # 构造请求体（根据 DeepSeek 文档调整参数结构）
        payload = {
            "model": MODEL_NAME,
            "messages": [
                {
                    "role": "system",
                    "content": "你是一个资深评论分析师，需根据用户的具体问题提供深入且多样化的分析。避免使用模板化回答，确保每次回答贴合问题细节。"
                },
                {
                    "role": "user",
                    "content": user_message
                }
            ],
            "temperature": 0.8,  # 适当提高温度值增强多样性
            "max_tokens": 1000,   # 根据需求调整输出长度
            "top_p": 0.9,         # 新增常见参数
            "stream": False       # 关闭流式传输
        }

        # 发送 POST 请求
        response = requests.post(
            url=DEEPSEEK_API_URL,
            headers=headers,
            json=payload,
            timeout=30  # 设置超时时间
        )

        # 处理 API 响应
        if response.status_code == 200:
            result = response.json()
            if 'choices' in result and len(result['choices']) > 0:
                reply_content = result['choices'][0]['message']['content']
                print("[Generated Reply]", reply_content[:100] + "...")  # 打印部分内容用于调试
                return JsonResponse({
                    'status': 'success',
                    'reply': reply_content
                })
            else:
                raise Exception("API 返回数据格式异常")
        else:
            error_info = response.json().get('error', {})
            raise Exception(f"API 请求失败: {error_info.get('message', '未知错误')}")

    except json.JSONDecodeError:
        return JsonResponse({
            'status': 'error',
            'message': '请求数据格式错误：需要 application/json'
        }, status=400)
    except Exception as e:
        print("[Error]", str(e))
        return JsonResponse({
            'status': 'error',
            'message': f'服务异常: {str(e)}'
        }, status=500)