from fastapi import FastAPI, APIRouter,Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import meilisearch

meili_api = 'https://data-ops.bpdash.repl.co'
meili_key = 'VIDURA_KEY'

meili = meilisearch.Client(meili_api, meili_key)
app = FastAPI()


app.mount("/static", StaticFiles(directory="userapp/static"), name="static")
templates = Jinja2Templates(directory="userapp/templates")

router = APIRouter(prefix="/api")


@router.get("/search")
async def search(q: str, page: int = 1):
    limit = 20
    offset = limit * (page - 1)
    index = meili.index('news-feed')
    res = index.search(q, {'offset': offset, 'limit': limit})
    print(res.keys())

    return res


@app.get("/")
def index(request: Request,response_class=HTMLResponse,q: str="", page: int = 1):
    limit = 20
    offset = limit * (page - 1)
    index = meili.index('news-feed')
    result = index.search(q, {'offset': offset, 'limit': limit})

    return templates.TemplateResponse("index.html", {"request": request,"query":q,"hits":result["hits"],"resultcount":result["estimatedTotalHits"],"timetaken":result["processingTimeMs"]/1000})

    
app.include_router(router)
