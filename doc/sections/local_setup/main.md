# Starting the environment

### Related Documentation
- [Project Main Documentation](../../../README.md)


## Setting up env

### Based on
- https://towardsdatascience.com/apache-spark-on-windows-a-docker-approach-4dd05d8a7147


## 1. Install Docker Desktop for windows
- https://www.docker.com/products/docker-desktop/

#### Check Docker installation
```bash
docker run hello-world
```

## 2. Create a Pyspark container
```bash
# Run with all as default
docker run -p 8888:8888 -e JUPYTER_ENABLE_LAB=yes --name pyspark jupyter/pyspark-notebook

# Run and specify to mount a local folder to be accessible within the container
docker run -it --rm -p 8888:8888 -e JUPYTER_ENABLE_LAB=yes --name pyspark_mounted -v F:/olivier/temp/test_json_ingest:/home/jovyan/data jupyter/pyspark-notebook 
```
- This will install and start a docker container which will provide a URL to access from any IDE. We will be using VS code in this example
- Obtain the URL of the jupyter-notebook
    - This is located in the container logs and should look like this
    - http://127.0.0.1:8888/lab?token=84510c774fc11c702ac4f8ed186ea77dec5677b2e3a9d850


## 3. Set the jupyter URL to VS code
- Open a notebook to work with
- Select the kernel (top right of the notebooks)
- Pick option `Select another kernel`
- Pick `Existing Jupyter Server`
- Pick `Enter URL of the running Jupyter Server`
- Enter URL of the jupyter-notebook
- Select 127.0.0.1
- Select the available kernel from there 

# Notes

### How to bind a local folder to a docker container
- https://docs.docker.com/guides/walkthroughs/access-local-folder/#:~:text=By%20default%2C%20containers%20can%27t,latest%20version%20of%20Docker%20Desktop.



### Usefull commands in docker
| Command  | Description |
| ------------- | ------------- |
| `docker images`  | Show the list of images available  |
| `docker ps`  | Show the list of containers  and their ID |
| `docker-compose -up -d`  | Run image on the current folder  |
| `docker exec -i -t [container id] /bin/bash`  | Opens bash of selected container ID  |

### Usefull commands inside the container
| Command  | Description |
| ------------- | ------------- |
| `jupyter server list`  | List the jupyter servers available  |



### Other notes

#### To disable windows virtual platform

Turn on Virtual Machine Platform in Windows
- Select Start, enter Windows features, and select Turn Windows features on or off from the list of results.
- In the Windows Features window that just opened, find Virtual Machine Platform and select it.
- Select OK. You might need to restart your PC.