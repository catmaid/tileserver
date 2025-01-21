import io
import numpy as np
import tensorstore as ts
import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from PIL import Image
import redis
import settings
import traceback


class AsyncTensorStoreServer:
    def __init__(self, tensorstore_spec: dict, tile_size: int = settings.TILE_SIZE):
        """
        Initialize the tensorstore server with a TensorStore volume.

        :param tensorstore_spec: TensorStore specification for loading the volume
        :param tile_size: Size of each 2D tile (default 256x256)
        """
        # Asynchronously open the TensorStore volume
        self.dataset = {}
        self.dataset_3d = {}
        self.shapes = {}
        self.dtypes = {}

        for zoom in settings.TENSORSTORE_SCALE_INDEX:
            # print('create tensorstore for zoom', zoom)
            specs = dict(tensorstore_spec)
            specs['scale_index'] = zoom
            self.dataset[zoom] = ts.open(
                specs,
                open=True,
                context=ts.Context(
                    {'cache_pool': {'total_bytes_limit': settings.TENSORSTORE_CACHE_SIZE}})
            ).result()
            self.dataset_3d[zoom] = self.dataset[zoom][ts.d['channel'][0]]
            self.shapes[zoom] = self.dataset_3d[zoom].shape
            self.dtypes[zoom] = self.dataset_3d[zoom].dtype

        self.tile_size = tile_size

    async def get_2d_tile(
        self,
        col: int,
        row: int,
        z: int,
        zoom: int = 0
    ) -> np.ndarray:
        """
        Asynchronously extract a 2D tile from a specific slice.

        :param x: X starting coordinate of tile
        :param y: Y starting coordinate of tile
        :param z: Z-slice coordinate to extract
        :param zoom: Zoom factor to extract data
        :return: 2D numpy array tile
        """
        # Calculate tile boundaries
        start_x = col * self.tile_size
        start_y = row * self.tile_size

        # Check bounds based on [x, y, z] shape
        # print('col, row, z, zoom', col, row, z, zoom)
        # print('start', start_x, start_y)
        # print('shapes', self.shapes[zoom])

        # tile outside of dataset, return black tile
        if start_x > self.shapes[zoom][0] or start_y > self.shapes[zoom][1]:
            tile = np.zeros((self.tile_size, self.tile_size), dtype=np.uint8)
            return tile

        # Calculate end coordinates, handling edge cases
        end_x = min(start_x + self.tile_size, self.shapes[zoom][0])
        end_y = min(start_y + self.tile_size, self.shapes[zoom][1])

        # print('range x', start_x, end_x)
        # print('range y', start_y, end_y)

        if z < 0 or z >= self.shapes[zoom][2]:
            raise ValueError(
                f"Tile out of z bounds: Request {z} for [0,{self.shapes[zoom][2]}]")

        if start_x >= self.shapes[zoom][0]:
            raise ValueError(
                f"Tile out of x bounds: Request {start_x} larger than {self.shapes[zoom][0]}]")

        if start_y >= self.shapes[zoom][1]:
            raise ValueError(
                f"Tile out of y bounds: Request {start_y} larger than {self.shapes[zoom][1]}]")

        # Asynchronously read the tile (note the axis order)
        tile = await self.dataset_3d[zoom][start_x:end_x, start_y:end_y, z].read()

        return tile.T


app = FastAPI()

tile_server = AsyncTensorStoreServer(settings.TENSORSTORE_SPEC)

r = redis.Redis(host=settings.DRAGONFLY_HOST,
                port=settings.DRAGONFLY_PORT, db=0)


@app.get("/volume_info/{zoom}")
async def volume_info(zoom: int):
    return {
        "shape": tile_server.shapes[zoom],
        "dtype": str(tile_server.dtypes[zoom]),
        "tile_size": tile_server.tile_size
    }


async def make_my_tile(zoom, z, row, col):
    key = f"/{zoom}/{z}/{row}/{col}.png"
    if r.exists(key):
        return
    try:
        # Extract tile
        tile = await tile_server.get_2d_tile(col, row, z, zoom)

        # Convert to image and stream
        pil_image = Image.fromarray(tile)
        img_byte_arr = io.BytesIO()
        pil_image.save(img_byte_arr, format='PNG')
        img_byte_arr.seek(0)

        # Write image to cache
        r.set(key, img_byte_arr.getvalue())

    except Exception as e:
        traceback.print_exc()
        raise Exception(e)


@app.get("/make_tile/{zoom}/{z}/{row}/{col}.png")
async def make_tile(
    row: int,
    col: int,
    z: int,
    zoom: int,
    background_tasks: BackgroundTasks
):
    try:
        await make_my_tile(zoom, z, row, col)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

    if settings.PREFETCH_ADJACENT_Z > 0:
        for i in range(settings.PREFETCH_ADJACENT_Z):
            background_tasks.add_task(make_my_tile, zoom, z+i, row, col)
            background_tasks.add_task(make_my_tile, zoom, z-i, row, col)

    if settings.PREFETCH_HIGHERRES_SCALE_INDEX:
        pass
        # TODO: col (x), row (y) with tile size at zoomlevel s
        # into col, row at zoomlevel s-1, s-2, ..., 0


def main():
    """
    Run the tensorstore server using uvicorn
    """
    uvicorn.run(
        "tensorstore_server:app",
        host=settings.TENSORSTORE_SERVER_HOST,
        port=settings.TENSORSTORE_SERVER_PORT,
        workers=settings.TENSORSTORE_SERVER_WORKERS,
        reload=settings.TENSORSTORE_SERVER_DEBUG,
        log_level=settings.TILESERVER_LOGLEVEL
    )


if __name__ == "__main__":
    main()
