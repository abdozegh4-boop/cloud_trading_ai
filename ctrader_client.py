import asyncio
import json
import websockets

CTRADER_HOST = "live.ctraderapi.com" # أو demo.ctraderapi.com
CTRADER_PORT = 5035

async def get_ctrader_data(symbol: str):
    """ربط مباشر وسريع عبر WebSockets بـ cTrader Open API"""
    uri = f"wss://{CTRADER_HOST}:{CTRADER_PORT}"
    try:
        async with websockets.connect(uri) as websocket:
            # إرسال طلب البيانات المصممة حسب بروتوكول cTrader
            payload = {
                "clientMsgId": "1",
                "payloadType": 2104, # ProtoOAAccountAuthReq كمثال
                "params": {"symbol": symbol}
            }
            await websocket.send(json.dumps(payload))
            response = await websocket.recv()
            return json.loads(response)
    except Exception as e:
        print(f"cTrader Connection Error: {e}")
        return None