# Automata Redis 
both for conventional key-value & timeseries data.

## Packaging
`python3 -m build`

## Commands
1. Timeseries Range (open ended) `TS.RANGE [KEY] 0 +`
2. Timeseries Range (latest value summarized) `TS.GET [KEY]`

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