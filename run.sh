service=GRobot
container=$service
img=tomkat/grobot:2

docker container stop $container
docker container rm $container 

docker run -dt \
    -p 8085:300 \
    -e DB_HOST=192.168.10.5 \
    -e DB_PATH=cars_dev \
    -e DB_USER=MONITOR \
    -e DB_PASSWORD=inwino \
    -e DELAY=2 \
    --name=$container \
    -e TZ=Europe/Kyiv \
    --restart=always \
    $img
