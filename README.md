# Automata Redis 
both for conventional key-value (including hashset) data.

## Packaging
`python3 -m build`

## LXD Container

### Create LXD container
1. `cd ~/projects/scripts/bash-scripts/lxc/`
2. `./lxc-create-basic-ubuntu-container.sh automata-all 10.104.71.60 /projects/code/automata-projects/automata-deploy`
3. `lxc.list`

Add these aliases to `vi ~/bash/bash-profile-aliases/aliases/bash-projects`
```
# automata all
alias automata-all.lxc.start="lxc.start-container automata-all"
alias automata-all.lxc.stop="lxc.stop-container automata-all"
alias automata-all.lxc.run-in="lxc.run-in.container automata-all"
alias automata-all.project="cd ~/projects/code/automata-projects/automata-deploy"
```
Remember to run `source ~/.bashrc`

### Container Info
* `lxc image list images: ubuntu/22.04 amd64`

### Container Manipulation
* `lxc stop automata-all`
* `lxc delete automata-all`

### Accessing Container
* `automata-all.lxc.run-in`

## Redis (Container)

### Redis Install
1. `sudo apt update`
2. `sudo apt install redis`

### Redis Config
1. `sudo vi /etc/redis/redis.conf`
2. Change to `bind 10.104.71.60 127.0.0.1` (allow second IP for accessing on host by default)
3. `sudo systemctl restart redis-server`

### Redis Port (outside container)
* `nc -zv 10.104.71.60 6379`

Stop and start the container to ensure redis, has installed correctly.

## Backup (Redis)
1. `CONFIG get dir` (in `redis-cli`) Tells where the dump file is located
2. `SAVE`
3. `/var/lib/redis` (should be `dump.rdb`)
4. `sudo systemctl status redis-server`
5. `sudo systemctl stop redis-server`
6. `sudo cp /var/lib/redis/dump.rdb BACKUP-DIR`

## Restore (Redis)
1. `sudo systemctl stop redis-server`
2. `sudo cp BACKUP-DIR /var/lib/redis/dump.rdb`
3. `sudo systemctl start redis-server`

## Docker

### Cache Docker Container (server)
* `docker start automata-data-db` (should start on port 6379)

### Redis Tools Docker Container (client)
* `docker run --name redis-insight --network automata-net -d -p 8001:8001 redislabs/redisinsight:latest`

