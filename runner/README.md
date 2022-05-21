# Speedtest and ping runner

Install [Speedtest CLI](https://www.speedtest.net/apps/cli)

Set the corrent path to python binaries in `speedtest.sh` and `ping.sh`

Run `which speedtest` and `which ping` to make ensure `speedtest.py` and `ping.py` are using the correct paths

Create a `.env` file providing AWS credentals (for writing to Amazon Kinesis) and a host name eg. RaspberryPiZero
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `HOST`

Make sure `speedtest` and `ping` shell scripts have execution permissions
```
chmod +x speedtest.sh
chmod +x ping.sh
```

Configure the cron job for the runner with the contents of `crontab.txt` 
```
crontab -e
```