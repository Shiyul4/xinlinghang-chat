import os
import json
import hmac
import base64
import hashlib
import asyncio
import logging
from pathlib import Path
from email.utils import formatdate
from urllib.parse import urlencode
from typing import Optional, Dict, Any, List, Tuple

import httpx
import websockets
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, UploadFile, File, Form, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# =========================
# 基础配置
# =========================
load_dotenv()

BASE_DIR = Path(__file__).parent.resolve()
STATIC_DIR = BASE_DIR / "static"
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

HOST = os.getenv("HOST", "127.0.0.1")
PORT = int(os.getenv("PORT", "8000"))
CORS_ALLOW_ORIGINS = os.getenv("CORS_ALLOW_ORIGINS", "*")

# =========================
# 日志
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("xinlinghang-assistant")

# =========================
# 阿里百炼 / DashScope
# =========================
BAILIAN_AGENT_APP_ID = os.getenv("BAILIAN_AGENT_APP_ID", "")
BAILIAN_AGENT_API_KEY = os.getenv("BAILIAN_AGENT_API_KEY", "")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "qwen-plus")
OPENAI_VL_MODEL = os.getenv("OPENAI_VL_MODEL", "qwen-vl-max-latest")

# =========================
# 讯飞 IAT (语音听写)
# =========================
XFYUN_IAT_APPID = os.getenv("XFYUN_IAT_APPID", "")
XFYUN_IAT_APIKEY = os.getenv("XFYUN_IAT_APIKEY", "")
XFYUN_IAT_APISECRET = os.getenv("XFYUN_IAT_APISECRET", "")
XFYUN_IAT_HOST = "ws-api.xfyun.cn"
XFYUN_IAT_PATH = "/v2/iat"
XFYUN_IAT_URL = f"wss://{XFYUN_IAT_HOST}{XFYUN_IAT_PATH}"


# =========================
# VMS 配置读取 (从 vms-config.js 文件读取)
# =========================
def load_vms_config() -> Dict[str, Any]:
    """从 static/vms-config.js 读取VMS配置"""
    vms_config_path = STATIC_DIR / "vms-config.js"
    default_config = {
        "appId": "",
        "apiKey": "",
        "apiSecret": "",
        "avatarId": "111188001",
        "vcn": "x4_yezi",
        "speed": 50,
        "pitch": 50,
        "volume": 55,
        "stream": {
            "protocol": "xrtc",
            "alpha": 1
        },
        "avatar_dispatch": {
            "interactive_mode": 0
        }
    }

    try:
        if not vms_config_path.exists():
            logger.warning(f"vms-config.js 不存在: {vms_config_path}")
            return default_config

        content = vms_config_path.read_text(encoding='utf-8')

        # 提取 JSON 部分 (去掉 window.VMS_CONFIG = 和末尾的分号)
        import re

        # 尝试匹配 JSON 对象
        match = re.search(r'window\.VMS_CONFIG\s*=\s*(\{[\s\S]*?\});?\s*$', content)
        if not match:
            # 尝试另一种格式 (VMS_CONFIG 不带 window.)
            match = re.search(r'VMS_CONFIG\s*=\s*(\{[\s\S]*?\});?\s*$', content)

        if match:
            json_str = match.group(1)
            config = json.loads(json_str)
            logger.info(f"✅ 已从 vms-config.js 加载VMS配置")
            return config
        else:
            logger.warning("无法在 vms-config.js 中找到有效的 JSON 配置")
            return default_config

    except json.JSONDecodeError as e:
        logger.error(f"解析 vms-config.js 失败 (JSON错误): {e}")
        return default_config
    except Exception as e:
        logger.error(f"读取 vms-config.js 失败: {e}")
        return default_config


# 全局VMS配置 (启动时加载，也可以每次请求时重新加载)
VMS_CONFIG = load_vms_config()

# =========================
# 企业配置
# =========================
DEFAULT_COMPANY_CONFIG = {
    "company_id": "xinlinghang",
    "company_name": "浙江新领航智能科技有限公司",
    "assistant_name": "新领航小E",
    "avatar": "/static/logo3.png",
    "remark": "低空经济 · 智能巡检 · 专业助手",
    "contact_phone": "15825634988",
    "contact_email": "contact@xinlinghang.com",
}

# =========================
# FastAPI
# =========================
app = FastAPI(title="新领航智能助手 API", version="5.1.0")

allow_origins = ["*"] if CORS_ALLOW_ORIGINS == "*" else [x.strip() for x in CORS_ALLOW_ORIGINS.split(",") if x.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================
# 数据模型
# =========================
class ChatRequest(BaseModel):
    message: str
    company_id: Optional[str] = None
    session_id: Optional[str] = None
    search_strategy: str = "agent"
    use_knowledge_base: bool = True
    answer_style: str = "detailed"


# =========================
# 工具函数
# =========================
def mask_key(key: str) -> str:
    if not key or len(key) < 12:
        return "未配置"
    return f"{key[:6]}...{key[-4:]}"


def file_to_data_url(content: bytes, content_type: str) -> str:
    encoded = base64.b64encode(content).decode("utf-8")
    return f"data:{content_type};base64,{encoded}"


# =========================
# 讯飞 IAT WebSocket 鉴权
# =========================
def build_xfyun_iat_auth_url() -> str:
    """
    讯飞 IAT WebSocket 鉴权 URL
    """
    date = formatdate(timeval=None, localtime=False, usegmt=True)
    signature_origin = f"host: {XFYUN_IAT_HOST}\n"
    signature_origin += f"date: {date}\n"
    signature_origin += f"GET {XFYUN_IAT_PATH} HTTP/1.1"

    signature_sha = hmac.new(
        XFYUN_IAT_APISECRET.encode("utf-8"),
        signature_origin.encode("utf-8"),
        digestmod=hashlib.sha256
    ).digest()

    signature = base64.b64encode(signature_sha).decode("utf-8")
    authorization_origin = (
        f'api_key="{XFYUN_IAT_APIKEY}", '
        f'algorithm="hmac-sha256", '
        f'headers="host date request-line", '
        f'signature="{signature}"'
    )
    authorization = base64.b64encode(authorization_origin.encode("utf-8")).decode("utf-8")

    params = {
        "authorization": authorization,
        "date": date,
        "host": XFYUN_IAT_HOST,
    }
    return f"{XFYUN_IAT_URL}?{urlencode(params)}"


def parse_iat_message(iat_message: Dict[str, Any]) -> Tuple[
    str, Optional[int], Optional[str], Optional[List[int]], int]:
    """
    解析讯飞 IAT 消息
    返回: piece, sn, pgs, rg, status
    """
    data = iat_message.get("data", {}) or {}
    result = data.get("result", {}) or {}
    ws_list = result.get("ws", []) or []

    parts: List[str] = []
    for item in ws_list:
        for cw in item.get("cw", []) or []:
            word = cw.get("w", "")
            if word:
                parts.append(word)
                break

    piece = "".join(parts)
    sn = result.get("sn")
    pgs = result.get("pgs")
    rg = result.get("rg")
    status = data.get("status", 1)
    return piece, sn, pgs, rg, status


def compose_iat_text(
        piece: str,
        sn: Optional[int],
        pgs: Optional[str],
        rg: Optional[List[int]],
        segments: Dict[int, str]
) -> str:
    """
    将讯飞返回按分段序号拼成完整文本
    尽量兼容普通模式与动态修正
    """
    # 如果 piece 为空且 sn 为 None，直接返回当前累积文本
    if not piece and sn is None:
        return "".join(segments[idx] for idx in sorted(segments.keys()))

    # 如果 sn 为 None，使用下一个序号
    if sn is None:
        sn = (max(segments.keys()) + 1) if segments else 0

    # 处理动态修正（替换模式）
    if pgs == "rpl" and isinstance(rg, list) and len(rg) == 2:
        start, end = rg
        for idx in range(start, end + 1):
            segments.pop(idx, None)
        # 只有当 piece 非空时才存储
        if piece:
            segments[end] = piece
    else:
        # 普通模式：只有当 piece 非空时才存储
        if piece:
            segments[sn] = piece

    return "".join(segments[idx] for idx in sorted(segments.keys()))


def build_iat_frame(audio_base64: str, status: int) -> Dict[str, Any]:
    """
    status:
      0 = first frame
      1 = continue frame
      2 = last frame
    """
    payload: Dict[str, Any] = {
        "data": {
            "status": status,
            "format": "audio/L16;rate=16000",
            "encoding": "raw",
            "audio": audio_base64
        }
    }

    if status == 0:
        payload["common"] = {"app_id": XFYUN_IAT_APPID}
        payload["business"] = {
            "domain": "iat",
            "language": "zh_cn",
            "accent": "mandarin",
            "vad_eos": 5000
        }

    return payload


# =========================
# DashScope / 百炼客户端
# =========================
class BailianAgentClient:
    def __init__(self):
        self.app_id = BAILIAN_AGENT_APP_ID
        self.api_key = BAILIAN_AGENT_API_KEY
        self.base_url = "https://dashscope.aliyuncs.com/api/v1"
        self.enabled = bool(self.app_id and self.api_key)

    async def completion(self, query: str, session_id: Optional[str] = None) -> Dict[str, Any]:
        if not self.enabled:
            raise RuntimeError("百炼智能体未配置")

        url = f"{self.base_url}/apps/{self.app_id}/completion"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        payload: Dict[str, Any] = {
            "input": {
                "prompt": query
            },
            "parameters": {
                "incremental_output": False
            }
        }
        if session_id:
            payload["input"]["session_id"] = session_id

        async with httpx.AsyncClient(timeout=90.0) as client:
            resp = await client.post(url, headers=headers, json=payload)
            if resp.status_code != 200:
                raise RuntimeError(f"百炼智能体调用失败: {resp.status_code} {resp.text[:200]}")
            return resp.json()


class OpenAICompatibleClient:
    def __init__(self):
        self.enabled = bool(OPENAI_API_KEY and OPENAI_BASE_URL)
        self.api_key = OPENAI_API_KEY
        self.base_url = OPENAI_BASE_URL.rstrip("/")

    async def chat_text(self, message: str, system_prompt: Optional[str] = None) -> str:
        if not self.enabled:
            raise RuntimeError("OpenAI 兼容接口未配置")

        messages: List[Dict[str, Any]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": message})

        payload = {
            "model": OPENAI_MODEL,
            "messages": messages,
            "temperature": 0.4
        }

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        async with httpx.AsyncClient(timeout=90.0) as client:
            resp = await client.post(f"{self.base_url}/chat/completions", headers=headers, json=payload)
            if resp.status_code != 200:
                raise RuntimeError(f"文本模型调用失败: {resp.status_code} {resp.text[:200]}")
            data = resp.json()
            return data["choices"][0]["message"]["content"]

    async def chat_vision(self, message: str, image_files: List[UploadFile], extra_file_names: List[str]) -> str:
        if not self.enabled:
            raise RuntimeError("OpenAI 兼容接口未配置")

        content: List[Dict[str, Any]] = []
        user_text = message.strip() or "请分析这张图片，并结合图片内容回答。"

        if extra_file_names:
            user_text += f"\n\n另有附件文件名：{', '.join(extra_file_names)}。如未读取到文件内容，请仅基于图片与文字进行回答。"

        content.append({"type": "text", "text": user_text})

        for file in image_files:
            raw = await file.read()
            file.file.seek(0)
            content.append({
                "type": "image_url",
                "image_url": {
                    "url": file_to_data_url(raw, file.content_type or "image/jpeg")
                }
            })

        payload = {
            "model": OPENAI_VL_MODEL,
            "messages": [
                {
                    "role": "user",
                    "content": content
                }
            ],
            "temperature": 0.3
        }

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(f"{self.base_url}/chat/completions", headers=headers, json=payload)
            if resp.status_code != 200:
                raise RuntimeError(f"视觉模型调用失败: {resp.status_code} {resp.text[:300]}")
            data = resp.json()
            return data["choices"][0]["message"]["content"]


agent_client = BailianAgentClient()
openai_client = OpenAICompatibleClient()

# session_id 存储：前端 session_id -> 百炼返回 session_id
SESSION_STORE: Dict[str, str] = {}


# =========================
# 统一对话逻辑 (使用阿里百炼智能体，不使用本地RAG)
# =========================
async def run_text_chat(user_message: str, frontend_session_id: Optional[str]) -> Dict[str, Any]:
    if not user_message.strip():
        raise HTTPException(status_code=400, detail="消息不能为空")

    # 优先使用百炼智能体（已包含阿里云端知识库）
    if agent_client.enabled:
        try:
            actual_session_id = SESSION_STORE.get(frontend_session_id or "", frontend_session_id)
            data = await agent_client.completion(user_message, actual_session_id)

            output = data.get("output", {})
            reply = output.get("text", "").strip()
            new_session_id = output.get("session_id") or actual_session_id

            if frontend_session_id and new_session_id:
                SESSION_STORE[frontend_session_id] = new_session_id

            # 获取引用来源
            doc_refs = output.get("docReferences", []) or []
            sources = []
            for ref in doc_refs[:5]:
                title = ref.get("title") or "百炼知识库"
                score = ref.get("score", 0)
                sources.append(f"{title} (相关度: {score:.2f})")

            return {
                "reply": reply or "未获取到模型回复。",
                "sources": sources,
                "strategy": "bailian_agent",
                "session_id": frontend_session_id or new_session_id
            }
        except Exception as e:
            logger.warning(f"百炼智能体失败，准备回退: {e}")

    # 回退到 OpenAI 兼容接口
    if openai_client.enabled:
        system_prompt = (
            "你是浙江新领航智能科技有限公司的智能助手'新领航小E'。"
            "回答时保持专业、清晰、简洁，聚焦低空经济、无人机巡检、智慧园区、智能巡检场景。"
        )
        reply = await openai_client.chat_text(user_message, system_prompt=system_prompt)
        return {
            "reply": reply,
            "sources": ["通用模型回答"],
            "strategy": "openai_compatible_text",
            "session_id": frontend_session_id
        }

    raise HTTPException(status_code=500, detail="未配置可用文本模型")


# =========================
# API路由
# =========================
@app.get("/")
async def root():
    return FileResponse(BASE_DIR / "index.html")


@app.get("/api/company-config")
async def get_company_config():
    """获取企业配置和VMS配置 (从 vms-config.js 读取)"""
    # 重新加载VMS配置（支持热更新）
    current_vms_config = load_vms_config()

    # 检查VMS是否已配置
    vms_enabled = bool(
        current_vms_config.get("appId") and
        current_vms_config.get("apiKey") and
        current_vms_config.get("apiSecret")
    )

    return {
        "avatar": DEFAULT_COMPANY_CONFIG["avatar"],
        "name": DEFAULT_COMPANY_CONFIG["assistant_name"],
        "remark": DEFAULT_COMPANY_CONFIG["remark"],
        "services": {
            "agent_enabled": agent_client.enabled,
            "voice_enabled": bool(XFYUN_IAT_APPID and XFYUN_IAT_APIKEY and XFYUN_IAT_APISECRET),
            "vms_enabled": vms_enabled,
            "multimodal_enabled": openai_client.enabled
        },
        # 直接传递 vms-config.js 中的配置给前端
        "vms_config": current_vms_config
    }


@app.get("/api/iat-auth")
async def get_iat_auth():
    """讯飞语音听写鉴权信息"""
    enabled = bool(XFYUN_IAT_APPID and XFYUN_IAT_APIKEY and XFYUN_IAT_APISECRET)
    return {
        "enabled": enabled,
        "provider": "xfyun_iat",
        "message": "ok" if enabled else "讯飞 IAT 未配置"
    }


@app.post("/api/chat")
async def chat(request: ChatRequest):
    """普通文本对话"""
    result = await run_text_chat(request.message, request.session_id)
    return result


@app.post("/api/chat-multimodal")
async def chat_multimodal(
        request_id: str = Form(...),
        message: str = Form(default=""),
        files: List[UploadFile] = File(default=[]),
        session_id: Optional[str] = Form(None)
):
    """多模态对话（支持图片上传）"""
    logger.info(f"收到多模态请求: request_id={request_id}, files={len(files)}")

    image_files: List[UploadFile] = []
    other_file_names: List[str] = []

    for f in files:
        ct = (f.content_type or "").lower()
        name = (f.filename or "").lower()
        if ct.startswith("image/") or name.endswith((".png", ".jpg", ".jpeg", ".webp", ".bmp")):
            image_files.append(f)
        else:
            other_file_names.append(f.filename or "unknown")

    # 如果有图片，使用视觉模型
    if image_files:
        try:
            reply = await openai_client.chat_vision(
                message=message,
                image_files=image_files,
                extra_file_names=other_file_names
            )
            return {
                "reply": reply,
                "sources": ["DashScope 视觉模型"],
                "strategy": "openai_compatible_vision",
                "session_id": session_id
            }
        except Exception as e:
            logger.warning(f"视觉模型失败，回退到文本逻辑: {e}")

    # 构造文本消息
    text_message = message.strip()
    if other_file_names:
        extra = f"\n\n附件文件名：{', '.join(other_file_names)}。如无法读取附件正文，请仅基于用户文字回答。"
        text_message = (text_message or "请根据以下附件文件名进行辅助理解。") + extra

    if not text_message.strip():
        text_message = "请总结我上传内容的用途。"

    result = await run_text_chat(text_message, session_id)
    return result


@app.post("/api/cancel/{request_id}")
async def cancel_request(request_id: str):
    """取消正在进行的请求"""
    logger.info(f"用户取消请求: {request_id}")
    return {"status": "cancelled", "request_id": request_id}


# =========================
# 讯飞 IAT WebSocket 中转 (语音输入核心)
# =========================
@app.websocket("/api/iat")
async def websocket_iat(websocket: WebSocket):
    await websocket.accept()

    if not (XFYUN_IAT_APPID and XFYUN_IAT_APIKEY and XFYUN_IAT_APISECRET):
        await websocket.send_json({
            "code": -1,
            "message": "讯飞 IAT 未配置"
        })
        await websocket.close()
        return

    xfyun_ws = None
    send_status = 0
    end_sent = False
    segments: Dict[int, str] = {}

    try:
        auth_url = build_xfyun_iat_auth_url()
        xfyun_ws = await websockets.connect(auth_url)
        logger.info("讯飞 IAT 已连接")

        async def client_to_xfyun():
            nonlocal send_status, end_sent
            try:
                while True:
                    data = await websocket.receive()

                    if isinstance(data, dict):
                        ws_type = data.get("type")

                        if ws_type == "websocket.disconnect":
                            logger.info("前端主动断开 IAT 连接")
                            if not end_sent:
                                await xfyun_ws.send(json.dumps(build_iat_frame("", 2)))
                                end_sent = True
                            break

                        if ws_type == "websocket.receive" and "text" in data and data["text"] is not None:
                            raw = data["text"]
                        else:
                            continue
                    else:
                        raw = data

                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    if msg.get("action") == "end":
                        if not end_sent:
                            await xfyun_ws.send(json.dumps(build_iat_frame("", 2)))
                            end_sent = True
                        break

                    audio_base64 = msg.get("audio")
                    if audio_base64 and not end_sent:
                        status_to_send = 0 if send_status == 0 else 1
                        await xfyun_ws.send(json.dumps(build_iat_frame(audio_base64, status_to_send)))
                        if send_status == 0:
                            send_status = 1

            except WebSocketDisconnect:
                logger.info("前端语音连接断开")
                if xfyun_ws and not end_sent:
                    try:
                        await xfyun_ws.send(json.dumps(build_iat_frame("", 2)))
                    except Exception:
                        pass
            except Exception as e:
                logger.warning(f"client_to_xfyun 异常: {e}")

        async def xfyun_to_client():
            try:
                async for raw in xfyun_ws:
                    data = json.loads(raw)
                    code = data.get("code", 0)

                    if code != 0:
                        await websocket.send_json({
                            "code": code,
                            "message": data.get("message", "讯飞识别失败")
                        })
                        break

                    piece, sn, pgs, rg, status = parse_iat_message(data)

                    # 修复：只有当 piece 非空时才更新文本
                    # 避免空字符串覆盖已有识别结果
                    if piece or sn is not None:
                        full_text = compose_iat_text(piece, sn, pgs, rg, segments)
                    else:
                        # 如果没有新内容，返回当前累积的文本
                        full_text = "".join(segments[idx] for idx in sorted(segments.keys()))

                    # 调试日志
                    logger.info(f"讯飞识别: piece='{piece}', sn={sn}, status={status}, full_text='{full_text[:50]}...'")

                    # 发送给前端 - 实时更新输入框
                    await websocket.send_json({
                        "code": 0,
                        "text": full_text,
                        "text_piece": piece,
                        "status": status
                    })

                    if status == 2:
                        break

            except Exception as e:
                logger.warning(f"xfyun_to_client 异常: {e}")
                try:
                    await websocket.send_json({
                        "code": -1,
                        "message": f"语音识别异常: {str(e)}"
                    })
                except Exception:
                    pass

        await asyncio.gather(client_to_xfyun(), xfyun_to_client())

    except Exception as e:
        logger.exception(f"/api/iat 异常: {e}")
        try:
            await websocket.send_json({
                "code": -1,
                "message": f"语音服务连接失败: {str(e)}"
            })
        except Exception:
            pass
    finally:
        try:
            if xfyun_ws:
                await xfyun_ws.close()
        except Exception:
            pass
        try:
            await websocket.close()
        except Exception:
            pass
        logger.info("IAT 会话结束")


# =========================
# 静态文件
# =========================
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# =========================
# 启动日志
# =========================
logger.info("=" * 60)
logger.info("新领航智能助手服务启动")
logger.info(f"地址: http://{HOST}:{PORT}")
logger.info(f"百炼智能体: {'已启用' if agent_client.enabled else '未启用'}")
logger.info(f"OpenAI兼容: {'已启用' if openai_client.enabled else '未启用'}")
logger.info(f"讯飞IAT: {'已启用' if (XFYUN_IAT_APPID and XFYUN_IAT_APIKEY and XFYUN_IAT_APISECRET) else '未启用'}")

# 显示VMS配置状态
vms_status = "已配置" if (VMS_CONFIG.get("appId") and VMS_CONFIG.get("apiKey")) else "未配置"
logger.info(f"VMS虚拟人: {vms_status} (从 vms-config.js 读取)")
if vms_status == "已配置":
    logger.info(f"  - avatarId: {VMS_CONFIG.get('avatarId', '默认')}")
    logger.info(f"  - vcn: {VMS_CONFIG.get('vcn', '默认')}")

logger.info("=" * 60)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)