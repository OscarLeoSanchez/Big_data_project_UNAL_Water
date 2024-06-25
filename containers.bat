# Define los directorios
$serverPath = "C:\Users\Oscar\Documents\Document\MastersAnalytics\BigData\evaluacion.curso\04-trabajo-final\UnalWater_project\server"
$dbPath = "C:\Users\Oscar\Documents\Document\MastersAnalytics\BigData\evaluacion.curso\04-trabajo-final\UnalWater_project\db"
$sparkPath = "C:\Users\Oscar\Documents\Document\MastersAnalytics\BigData\evaluacion.curso\04-trabajo-final\UnalWater_project\spark"

# Cambia al directorio del servidor y lanza el contenedor
Set-Location $serverPath
docker run -d --name unalwater_server --network unalwater_network -v ${PWD}:/app unalwater_server

# Cambia al directorio de la base de datos y lanza el contenedor
Set-Location $dbPath
docker run -d --name unalwater_db -e POSTGRES_PASSWORD=root -e POSTGRES_USER=user -e POSTGRES_DB=unalwater --network unalwater_network -p 5431:5432 unalwater_db

# Cambia al directorio de Spark y lanza el contenedor
Set-Location $sparkPath
docker run --rm -it --name spark -p 4040:4040 -p 5007:50070 -p 8088:8088 -p 8888:8888 --network unalwater_network -v ${PWD}:/workspace jdvelasq/spark:3.1.3

Write-Host "Todos los contenedores han sido lanzados con éxito."



