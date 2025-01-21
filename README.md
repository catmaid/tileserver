# TileServer with Tensorstore

A tile server for CATMAID [tile source type 4](https://catmaid.readthedocs.io/en/stable/tile_sources.html#file-based-image-stack-with-zoom-level-directories) to create and cache tiles on-the-fly from array data accessed using [TensorStore](https://google.github.io/tensorstore/index.html) supporting datasets in precomputed, N5, Zarr formats.


## Setup

- Create a Python Virtual Environment with the libraries in `requirements.txt`

`pip install -r requirements.txt`

- Configure `settings.py` and configure the server and dataset

`cp settings.py.example settings.py`

- If you server data from a Google Cloud bucket, you may have to configure access credentials, using [gloud](https://cloud.google.com/sdk/docs/install#linux)

`gcloud init`

- Run Redis-based Cache such as DragonFlyDB with Docker

```
docker run \
  --name dragonfly_tileserver \
  --cpus=2 \
  -p 6377:6379 \
  --ulimit memlock=-1 \
  docker.dragonflydb.io/dragonflydb/dragonfly \
  --cache_mode \
  --maxmemory=2g
```

This cache stores the generated tile images. Make sure that you provide it with sufficient maximal memory and cpu resources according to expected usage. More details can be found in the [DragonFlyDB documentation](https://www.dragonflydb.io/docs/managing-dragonfly/flags).

- Start the TileServer

`python tile_server.py`

Make sure to provide sufficient amount of workers to handle parallel tile requests

- Start the TensorstoreServer

`python tensorstore_server.py`
