import io
import uvicorn
from fastapi import FastAPI
from starlette.responses import StreamingResponse
import requests
import redis
import settings

app = FastAPI()

r = redis.Redis(host=settings.DRAGONFLY_HOST,
                port=settings.DRAGONFLY_PORT, db=0)


@app.get("/tile/{zoom}/{z}/{row}/{col}.png")
async def get_tile(
    row: int,  # y axis
    col: int,  # x axis
    z: int,
    zoom: int
):
    """
    Return tile from cache
    Ensure surrounding extent of tiles associated with the requested tile also exist
    """
    k = f"/{zoom}/{z}/{row}/{col}.png"

    requests.get(
        f'{settings.TENSORSTORE_SERVER_URL}/make_tile/{zoom}/{z}/{row}/{col}.png').json()

    img_bytes = r.get(k)

    return StreamingResponse(
        io.BytesIO(img_bytes),
        media_type="image/png"
    )


@app.get("/volume-info/{zoom}")
async def get_volume_info(zoom: int):
    """
    Provide metadata about the loaded 3D volume.
    """
    return requests.get(f'{settings.TENSORSTORE_SERVER_URL}/volume_info/{zoom}').json()


def main():
    """
    Run the tile server using uvicorn
    """
    uvicorn.run(
        "tile_server:app",
        host=settings.TILESERVER_HOST,
        port=settings.TILESERVER_PORT,
        workers=settings.TILESERVER_WORKERS,
        reload=settings.TILESERVER_DEBUG,
        log_level=settings.TILESERVER_LOGLEVEL
    )


if __name__ == "__main__":
    main()
