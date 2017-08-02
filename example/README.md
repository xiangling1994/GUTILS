An example GDAM implementation using `docker-compose`

* `./config` - put individual glider config files in here (eg. `./config/usf-bass-dep1/*.json`)
* `./binary` - put your glider data in here (eg. `./data/usf-bass-dep1/*.tdb`)
* `./ascii` - netCDF files are produced here
* `./netcdf` - netCDF files are produced here
* `./ftp` - files uploaded to the ftp are produced here

### Running



```bash
$ docker-compose build  # If you make changes to the code you need to run this everytime
$ docker-compose up
```
