from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from request_api import RequestInfo, TokenPriceInfo, NftInfo

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/insert_nft")
async def insert_nft():
    data = RequestInfo()
    return {"code": 200, "message": data.insert_nft_sales()}


@app.get("/insert_price")
async def insert_price():
    data = RequestInfo()
    return {"code": 200, "message": data.insert_token_prices()}


@app.get("/get_price")
async def get_price(token_address: str, days: int):
    data = TokenPriceInfo()
    result = data.get_price(token_address, days)
    return {"code": 200, "data": result}


@app.get("/get_nft_price")
async def get_nft_price(time_interval: int):
    data = NftInfo()
    result = data.get_nft_price(time_interval)
    return {"code": 200, "data": result}
