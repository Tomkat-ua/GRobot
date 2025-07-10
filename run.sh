service=GRobot
container=$service
img=tomkat/grobot:1

docker container stop $container
docker container rm $container 

docker run -dt \
    -e DB_HOST=192.168.10.5 \
    -e DB_PATH=cars_dev \
    -e DELAY=1 \
    --name=$container \
    -e TZ=Europe/Kyiv \
    --restart=always \
    $img
